# -*- coding: utf-8 -*-
import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import logging
import threading
import re
import sys
import atexit
import requests
import pymongo
from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId
# Import exception handler for Telegram API errors
from telebot.apihelper import ApiTelegramException
# Import Flask for Render Health Checks
from flask import Flask

# --- Configuration ---
TOKEN = os.getenv('TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
OWNER_ID = 6059117268
ADMIN_ID = 6059117268
YOUR_USERNAME = 'yoriichi62'
UPDATE_CHANNEL = 'Hopper_updeter'

# Folder setup
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
# IROTECH_DIR is no longer needed for SQLite, but we might use it for temp storage if needed
IROTECH_DIR = os.path.join(BASE_DIR, 'inf') 

# File upload limits
FREE_USER_LIMIT = 3
SUBSCRIBED_USER_LIMIT = 15
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Web Server for Render (Keep Alive) ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!", 200

def run_web_server():
    # Render provides the PORT environment variable
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# --- MongoDB Setup ---
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client['bot_hosting_db']
    fs = gridfs.GridFS(db)
    
    # Collections
    users_col = db['users']             # Stores user info, subs
    admins_col = db['admins']           # Stores admin IDs
    user_files_col = db['user_files']   # Metadata for files
    active_users_col = db['active_users'] # List of active user IDs

    # Indexes for performance
    user_files_col.create_index([("user_id", 1), ("file_name", 1)], unique=True)
    
    logging.info("✅ Connected to MongoDB Atlas successfully.")
except Exception as e:
    logging.critical(f"❌ Failed to connect to MongoDB: {e}")
    sys.exit(1) # Exit if DB fails

# --- Data structures (Memory Cache) ---
# We keep these in memory for fast access, but sync with DB
bot_scripts = {} # {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
active_users = set() 
admin_ids = {ADMIN_ID, OWNER_ID} 
bot_locked = False

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["📞 Contact Owner"]
]
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"],
    ["👑 Admin Panel", "📞 Contact Owner"]
]

# --- Database & Persistence Loading ---
def load_data():
    """Load data from MongoDB into memory"""
    logger.info("Loading data from MongoDB...")
    global user_subscriptions, user_files, active_users, admin_ids

    try:
        # Load Subscriptions
        # Schema: {_id: user_id, expiry: datetime}
        subs = users_col.find({"expiry": {"$exists": True}})
        for doc in subs:
            user_subscriptions[doc['_id']] = {'expiry': doc['expiry']}

        # Load User Files
        # Schema: {user_id: 123, file_name: "bot.py", file_type: "py", source_id: ObjectId, is_zip: bool}
        files = user_files_col.find()
        for doc in files:
            uid = doc['user_id']
            if uid not in user_files: user_files[uid] = []
            user_files[uid].append((doc['file_name'], doc['file_type']))

        # Load Active Users
        # Schema: {_id: user_id}
        actives = active_users_col.find()
        active_users.update(doc['_id'] for doc in actives)

        # Load Admins
        # Schema: {_id: user_id}
        admins = admins_col.find()
        admin_ids.update(doc['_id'] for doc in admins)
        
        # Ensure Owner is admin
        if OWNER_ID not in admin_ids:
            add_admin_db(OWNER_ID)

        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subs, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data from Mongo: {e}", exc_info=True)

load_data()

# --- Helper Functions ---
def get_user_folder(user_id):
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name):
    script_key = f"{script_owner_id}_{file_name}"
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                cleanup_zombie_entry(script_key, script_info)
            return is_running
        except psutil.NoSuchProcess:
            cleanup_zombie_entry(script_key, script_info)
            return False
    return False

def cleanup_zombie_entry(script_key, script_info):
    if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
        try: script_info['log_file'].close()
        except: pass
    if script_key in bot_scripts: del bot_scripts[script_key]

def kill_process_tree(process_info):
    pid = None
    script_key = process_info.get('script_key', 'N/A')
    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try: process_info['log_file'].close()
            except: pass

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
            pid = process.pid
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            for child in children:
                try: child.terminate()
                except: pass
            psutil.wait_procs(children, timeout=1)
            try: parent.terminate()
            except: pass
            try: parent.wait(timeout=1)
            except: parent.kill()
    except Exception as e:
        logger.error(f"Error killing process {pid} ({script_key}): {e}")

# --- File Restoration from DB ---
def ensure_file_on_disk(user_id, file_name, user_folder):
    """
    Checks if file exists locally. If not, fetches from MongoDB GridFS.
    Handles ZIP extraction if the source was a ZIP.
    """
    local_path = os.path.join(user_folder, file_name)
    if os.path.exists(local_path):
        return True # File exists, all good

    logger.info(f"File {file_name} not found locally for {user_id}. Attempting restore from MongoDB...")
    
    # Fetch metadata
    doc = user_files_col.find_one({"user_id": user_id, "file_name": file_name})
    if not doc:
        logger.error(f"Metadata missing for {file_name} (User {user_id})")
        return False

    source_id = doc.get('source_id')
    is_zip = doc.get('is_zip', False)

    if not source_id or not fs.exists(source_id):
        logger.error(f"Source file missing in GridFS for {file_name} (User {user_id})")
        return False

    try:
        # Download source
        grid_out = fs.get(source_id)
        
        if is_zip:
            temp_zip_path = os.path.join(user_folder, f"temp_restore_{user_id}.zip")
            with open(temp_zip_path, 'wb') as f:
                f.write(grid_out.read())
            
            # Extract
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(user_folder)
            os.remove(temp_zip_path)
            logger.info(f"Restored ZIP content for {file_name}")
        else:
            with open(local_path, 'wb') as f:
                f.write(grid_out.read())
            logger.info(f"Restored single file {file_name}")
            
        return os.path.exists(local_path)
    except Exception as e:
        logger.error(f"Failed to restore file {file_name}: {e}", exc_info=True)
        return False

# --- Auto Install & Run ---
def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: return False
    try:
        bot.reply_to(message, f"🐍 Installing `{package_name}`...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', package_name], check=True, capture_output=True)
        bot.reply_to(message, f"✅ Installed `{package_name}`.")
        return True
    except Exception as e:
        bot.reply_to(message, f"❌ Install failed: {e}")
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Installing `{module_name}`...")
        subprocess.run(['npm', 'install', module_name], cwd=user_folder, check=True, capture_output=True)
        bot.reply_to(message, f"✅ Installed `{module_name}`.")
        return True
    except Exception as e:
        bot.reply_to(message, f"❌ Install failed: {e}")
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    if attempt > 2: return

    # Ensure file exists (Auto-restore from Mongo if Render wiped disk)
    if not ensure_file_on_disk(script_owner_id, file_name, user_folder):
        bot.reply_to(message_obj_for_reply, "❌ Error: File not found locally or in Database.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    
    # Pre-check logic (simplified)
    if attempt == 1:
        try:
            check_proc = subprocess.Popen([sys.executable, script_path], cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                _, stderr = check_proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                check_proc.kill(); stderr = ""
            
            if check_proc.returncode != 0 and stderr:
                match = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                if match and attempt_install_pip(match.group(1), message_obj_for_reply):
                    threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                    return
        except Exception: pass

    # Execution
    log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
    try:
        log_file = open(log_path, 'w', encoding='utf-8', errors='ignore')
        process = subprocess.Popen([sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file, stdin=subprocess.PIPE)
        
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'script_owner_id': script_owner_id, 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid})")
    except Exception as e:
        if 'log_file' in locals() and log_file: log_file.close()
        bot.reply_to(message_obj_for_reply, f"❌ Failed to start: {e}")

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    if attempt > 2: return
    
    # Ensure file exists
    if not ensure_file_on_disk(script_owner_id, file_name, user_folder):
        bot.reply_to(message_obj_for_reply, "❌ Error: File not found locally or in Database.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    
    # Execution
    log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
    try:
        log_file = open(log_path, 'w', encoding='utf-8', errors='ignore')
        process = subprocess.Popen(['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file, stdin=subprocess.PIPE)
        
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'script_owner_id': script_owner_id, 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid})")
    except Exception as e:
        if 'log_file' in locals() and log_file: log_file.close()
        bot.reply_to(message_obj_for_reply, f"❌ Failed to start: {e}")

# Mappings for PIP
TELEGRAM_MODULES = {'telebot': 'pyTelegramBotAPI', 'telegram': 'python-telegram-bot', 'aiogram': 'aiogram', 'requests': 'requests', 'psutil': 'psutil'}

# --- Database Operations (MongoDB) ---

def save_user_file_mongo(user_id, file_name, file_type, source_path, is_zip=False):
    """
    Uploads file to GridFS and saves metadata to MongoDB.
    """
    try:
        # 1. Delete existing if any to avoid duplicates in GridFS/Meta
        remove_user_file_db(user_id, file_name)

        # 2. Upload content to GridFS
        with open(source_path, 'rb') as f:
            grid_id = fs.put(f, filename=os.path.basename(source_path), user_id=user_id)

        # 3. Save Metadata
        user_files_col.insert_one({
            "user_id": user_id,
            "file_name": file_name,
            "file_type": file_type,
            "source_id": grid_id,
            "is_zip": is_zip,
            "uploaded_at": datetime.now()
        })

        # 4. Update Memory Cache
        if user_id not in user_files: user_files[user_id] = []
        user_files[user_id].append((file_name, file_type))
        
        logger.info(f"Saved {file_name} to Mongo GridFS for {user_id}")
    except Exception as e:
        logger.error(f"Error saving file to Mongo: {e}", exc_info=True)

def remove_user_file_db(user_id, file_name):
    """
    Removes file from GridFS and metadata from MongoDB.
    """
    try:
        # Find the doc
        doc = user_files_col.find_one({"user_id": user_id, "file_name": file_name})
        if doc:
            # Delete from GridFS
            if 'source_id' in doc:
                try: fs.delete(doc['source_id'])
                except: pass # Might already be gone
            
            # Delete Metadata
            user_files_col.delete_one({"_id": doc['_id']})
            
            # Update Memory Cache
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
            
            logger.info(f"Removed {file_name} from Mongo for {user_id}")
    except Exception as e:
        logger.error(f"Error removing file from Mongo: {e}", exc_info=True)

def add_active_user(user_id):
    active_users.add(user_id)
    try: active_users_col.update_one({"_id": user_id}, {"$set": {"last_seen": datetime.now()}}, upsert=True)
    except: pass

def save_subscription(user_id, expiry):
    try:
        users_col.update_one({"_id": user_id}, {"$set": {"expiry": expiry}}, upsert=True)
        user_subscriptions[user_id] = {'expiry': expiry}
    except Exception as e: logger.error(f"DB Error save_subscription: {e}")

def remove_subscription_db(user_id):
    try:
        users_col.update_one({"_id": user_id}, {"$unset": {"expiry": ""}})
        if user_id in user_subscriptions: del user_subscriptions[user_id]
    except Exception as e: logger.error(f"DB Error remove_subscription: {e}")

def add_admin_db(admin_id):
    try:
        admins_col.update_one({"_id": admin_id}, {"$set": {"added_at": datetime.now()}}, upsert=True)
        admin_ids.add(admin_id)
    except: pass

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID: return False
    try:
        res = admins_col.delete_one({"_id": admin_id})
        if res.deleted_count > 0:
            admin_ids.discard(admin_id)
            return True
        return False
    except: return False

# --- Menu creation ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]
    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription'),
            types.InlineKeyboardButton('📊 Statistics', callback_data='stats'),
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'),
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'),
            types.InlineKeyboardButton('🟢 Run All User Scripts', callback_data='run_all_scripts')
        ]
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3], admin_buttons[0])
        markup.add(admin_buttons[1], admin_buttons[3])
        markup.add(admin_buttons[2], admin_buttons[5])
        markup.add(admin_buttons[4])
        markup.add(buttons[4])
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats'))
        markup.add(buttons[4])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
                   types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}'))
        markup.row(types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
                   types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}'))
    else:
        markup.row(types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
                   types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'))
        markup.row(types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
               types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription'))
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
               types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin'))
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

# --- File Handling Logic ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    
    # 1. Save ZIP locally (temp)
    temp_zip_path = os.path.join(user_folder, file_name_zip)
    with open(temp_zip_path, 'wb') as f: f.write(downloaded_file_content)
    
    try:
        # 2. Extract
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(user_folder)
        
        # 3. Find main script
        extracted_items = os.listdir(user_folder)
        main_script = next((f for f in extracted_items if f.endswith('.py') or f.endswith('.js')), None)
        
        if main_script:
            file_type = 'py' if main_script.endswith('.py') else 'js'
            
            # 4. Save to Mongo (The ZIP is the source)
            # We save the ZIP file to GridFS, but link it to the main_script name in metadata
            save_user_file_mongo(user_id, main_script, file_type, temp_zip_path, is_zip=True)
            
            bot.reply_to(message, f"✅ ZIP extracted. Main script: `{main_script}` saved to DB.")
            
            # Auto start
            if file_type == 'py':
                threading.Thread(target=run_script, args=(os.path.join(user_folder, main_script), user_id, user_folder, main_script, message)).start()
            else:
                threading.Thread(target=run_js_script, args=(os.path.join(user_folder, main_script), user_id, user_folder, main_script, message)).start()
        else:
            bot.reply_to(message, "❌ No .py or .js found in ZIP.")
            
    except zipfile.BadZipFile:
        bot.reply_to(message, "❌ Invalid ZIP file.")
    finally:
        # Cleanup temp zip if needed, but we used it for upload
        if os.path.exists(temp_zip_path): os.remove(temp_zip_path)

def handle_single_file(downloaded_file_content, file_name, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    file_path = os.path.join(user_folder, file_name)
    
    # 1. Save locally
    with open(file_path, 'wb') as f: f.write(downloaded_file_content)
    
    # 2. Save to Mongo
    file_type = 'py' if file_name.endswith('.py') else 'js'
    save_user_file_mongo(user_id, file_name, file_type, file_path, is_zip=False)
    
    bot.reply_to(message, f"✅ File `{file_name}` saved to DB.")
    
    # Auto start
    if file_type == 'py':
        threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, message)).start()
    else:
        threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, message)).start()

# --- Logic Functions ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    if user_id not in active_users: add_active_user(user_id)
    
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    status = "⭐ Premium" if user_id in user_subscriptions else ("🛡️ Admin" if user_id in admin_ids else "🆓 Free")
    
    text = (f"〽️ Welcome!\n🆔 `{user_id}`\n🔰 Status: {status}\n"
            f"📁 Files: {current_files} / {file_limit}\n\n"
            f"🤖 MongoDB Persistence Active.")
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row in layout: markup.add(*[types.KeyboardButton(t) for t in row])
    
    bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='Markdown')

def _logic_check_files(message):
    user_id = message.from_user.id
    files = user_files.get(user_id, [])
    if not files:
        bot.reply_to(message, "📂 No files uploaded.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for fname, ftype in files:
        status = "🟢 Running" if is_bot_running(user_id, fname) else "🔴 Stopped"
        markup.add(types.InlineKeyboardButton(f"{fname} ({ftype}) - {status}", callback_data=f'file_{user_id}_{fname}'))
    bot.reply_to(message, "📂 Your files:", reply_markup=markup)

def _logic_run_all_scripts(message):
    # Admin tool to restart all bots (fetches from Mongo if needed)
    if message.from_user.id not in admin_ids: return
    bot.reply_to(message, "⏳ Restoring and starting all scripts...")
    
    count = 0
    # Refresh global list from DB
    files_cursor = user_files_col.find()
    
    for doc in files_cursor:
        uid = doc['user_id']
        fname = doc['file_name']
        ftype = doc['file_type']
        
        if is_bot_running(uid, fname): continue
        
        user_folder = get_user_folder(uid)
        local_path = os.path.join(user_folder, fname)
        
        # This will trigger download from Mongo if missing locally
        if ftype == 'py':
            threading.Thread(target=run_script, args=(local_path, uid, user_folder, fname, message)).start()
        elif ftype == 'js':
            threading.Thread(target=run_js_script, args=(local_path, uid, user_folder, fname, message)).start()
        count += 1
        time.sleep(0.5)
        
    bot.reply_to(message, f"✅ Triggered start for {count} scripts.")

# --- Handlers ---
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m): _logic_send_welcome(m)

@bot.message_handler(content_types=['document'])
def handle_docs(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids: return
    
    limit = get_user_file_limit(user_id)
    if get_user_file_count(user_id) >= limit:
        bot.reply_to(message, "⚠️ File limit reached.")
        return

    doc = message.document
    file_name = doc.file_name
    if not file_name or not (file_name.endswith('.py') or file_name.endswith('.js') or file_name.endswith('.zip')):
        bot.reply_to(message, "⚠️ Only .py, .js, .zip allowed.")
        return

    file_info = bot.get_file(doc.file_id)
    downloaded = bot.download_file(file_info.file_path)
    
    if file_name.endswith('.zip'):
        handle_zip_file(downloaded, file_name, message)
    else:
        handle_single_file(downloaded, file_name, message)

@bot.callback_query_handler(func=lambda call: True)
def callback_router(call):
    data = call.data
    user_id = call.from_user.id
    
    if data == 'check_files': 
        _logic_check_files(call.message)
    elif data.startswith('file_'):
        # Show file controls
        _, uid, fname = data.split('_', 2)
        if int(uid) != user_id and user_id not in admin_ids: return
        running = is_bot_running(int(uid), fname)
        bot.edit_message_text(f"⚙️ {fname}", call.message.chat.id, call.message.message_id, 
                              reply_markup=create_control_buttons(int(uid), fname, running))
    
    elif data.startswith('delete_'):
        # DELETE LOGIC: Remove from Disk AND MongoDB
        _, uid_str, fname = data.split('_', 2)
        uid = int(uid_str)
        if uid != user_id and user_id not in admin_ids: return
        
        # Stop first
        script_key = f"{uid}_{fname}"
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            del bot_scripts[script_key]
        
        # Remove from MongoDB (GridFS + Metadata)
        remove_user_file_db(uid, fname)
        
        # Remove from local disk
        user_folder = get_user_folder(uid)
        try: os.remove(os.path.join(user_folder, fname))
        except: pass
        try: os.remove(os.path.join(user_folder, f"{os.path.splitext(fname)[0]}.log"))
        except: pass
        
        bot.answer_callback_query(call.id, "Deleted from Disk and DB.")
        _logic_check_files(call.message)

    elif data.startswith('start_'):
        _, uid, fname = data.split('_', 2)
        uid = int(uid)
        user_folder = get_user_folder(uid)
        path = os.path.join(user_folder, fname)
        
        if fname.endswith('.py'):
            threading.Thread(target=run_script, args=(path, uid, user_folder, fname, call.message)).start()
        else:
            threading.Thread(target=run_js_script, args=(path, uid, user_folder, fname, call.message)).start()
        bot.answer_callback_query(call.id, "Starting...")

    elif data.startswith('stop_'):
        _, uid, fname = data.split('_', 2)
        script_key = f"{uid}_{fname}"
        if script_key in bot_scripts:
            kill_process_tree(bot_scripts[script_key])
            del bot_scripts[script_key]
        bot.answer_callback_query(call.id, "Stopped.")
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 
                                      reply_markup=create_control_buttons(int(uid), fname, False))
                                      
    elif data == 'run_all_scripts':
        _logic_run_all_scripts(call.message)
    
    # ... (Include other admin/nav callbacks from original if needed) ...
    elif data == 'back_to_main':
        bot.edit_message_text("Main Menu", call.message.chat.id, call.message.message_id, 
                              reply_markup=create_main_menu_inline(user_id))

# --- Shutdown Cleanup ---
def cleanup():
    for key in list(bot_scripts.keys()):
        kill_process_tree(bot_scripts[key])

atexit.register(cleanup)

if __name__ == '__main__':
    # Force remove webhook to switch to polling mode
    try:
        bot.remove_webhook()
        time.sleep(1) # Give it a second to register
    except Exception as e:
        logger.error(f"Error removing webhook: {e}")

    # Start Flask Server in a separate thread to satisfy Render's port requirement
    threading.Thread(target=run_web_server, daemon=True).start()
    logger.info("🌍 Web server started on background thread.")

    # Robust Polling Loop
    while True:
        try:
            logger.info("🤖 Starting polling...")
            bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
        except ApiTelegramException as e:
            # Check for conflict error (Error 409)
            if "Conflict: terminated by other getUpdates request" in str(e):
                logger.warning("⚠️ Conflict detected: Another instance is running. Retrying in 10 seconds...")
                time.sleep(10) # Wait for other instance to die
            else:
                logger.error(f"❌ Telegram API Error: {e}")
                time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Polling crashed: {e}")
            time.sleep(5)
