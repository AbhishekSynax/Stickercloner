import os
import json
import random
import asyncio
import re
import string
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, InputSticker
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler
from telegram.error import TelegramError, BadRequest, RetryAfter

# --- CONFIG ---
BOT_TOKEN = "8317097040:AAE9VO4U5JQiKeR6p83OVzXt1sbceB1HLYU"
ADMIN_ID = 6068463116 
DB_FILE = "sticker_pro_db.json"
MAX_STICKERS_PER_PACK = 120  # Telegram limit
BATCH_SIZE = 50  # Process stickers in batches to avoid timeouts
RATE_LIMIT_NORMAL = 3  # Maximum packs per hour for normal users
RATE_LIMIT_PREMIUM = 10  # Maximum packs per hour for premium users
RATE_LIMIT_OWNER = 999  # Unlimited for owner

# --- DATABASE ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {
            "users": {}, 
            "settings": {
                "channels": [], 
                "limit": 2, 
                "support": "@Admin",
                "force_join_for": ["clone", "redeem"],  # Actions requiring channel join
                "max_days_per_code": 365,  # Maximum days a code can provide
                "max_codes_per_user": 10,  # Maximum codes a user can claim
                "admin_code_gen_limit": 50,  # Maximum codes an admin can generate per day
                "code_categories": ["General", "Special", "VIP", "Promo"]  # Code categories
            },
            "user_limits": {},  # Track rate limits
            "redeem_codes": {},  # Track redeem codes
            "code_templates": {},  # Predefined code templates
            "code_stats": {
                "total_generated": 0,
                "total_claimed": 0,
                "total_days": 0,
                "unique_users_claimed": 0,
                "daily_generated": {},  # Track daily generation by admin
                "daily_claimed": {}  # Track daily claims by users
            },
            "admin_actions": {}  # Track admin actions for rate limiting
        }
    with open(DB_FILE, "r") as f: 
        return json.load(f)

def save_db(data):
    with open(DB_FILE, "w") as f: 
        json.dump(data, f, indent=4)

# --- STATES ---
LNK, NME, BDC, SEL_STICKERS, REDEEM, GEN_CODE_NAME, GEN_CODE_DAYS, GEN_CODE_LIMIT, GEN_CODE_EXPIRY, GEN_CODE_CATEGORY, MANAGE_CHANNELS, BULK_GEN, GEN_CODE_TYPE, TEMPLATE_SELECT = range(14)
CANCEL = ConversationHandler.END

# --- OWNER CHECK ---
def is_owner(uid):
    """Check if user is the bot owner"""
    return str(uid) == str(ADMIN_ID)

# --- SUB CHECK ---
async def is_sub(bot, uid):
    db = load_db()
    for ch in db["settings"]["channels"]:
        try:
            m = await bot.get_chat_member(ch, uid)
            if m.status in ["left", "kicked"]: 
                return False
        except: 
            return False
    return True

# --- PREMIUM CHECK ---
def is_premium(uid):
    db = load_db()
    uid = str(uid)
    
    if uid not in db["users"]:
        return False
    
    # Check if user has premium and it's not expired
    if "premium_expires" in db["users"][uid]:
        expiry_date = datetime.fromisoformat(db["users"][uid]["premium_expires"])
        if expiry_date > datetime.now():
            return True
    
    return False

# --- RATE LIMIT CHECK ---
async def check_rate_limit(uid, action="clone"):
    """Check rate limit based on user plan and action"""
    # Owner has unlimited access
    if is_owner(uid):
        return True
    
    db = load_db()
    uid = str(uid)
    now = datetime.now().timestamp()
    
    if uid not in db["user_limits"]:
        db["user_limits"][uid] = {}
    
    if action not in db["user_limits"][uid]:
        db["user_limits"][uid][action] = []
    
    # Remove timestamps older than 1 hour
    db["user_limits"][uid][action] = [t for t in db["user_limits"][uid][action] if now - t < 3600]
    
    # Set rate limit based on user plan
    if action == "clone":
        rate_limit = RATE_LIMIT_PREMIUM if is_premium(uid) else RATE_LIMIT_NORMAL
    elif action == "redeem":
        # Check if user has reached their maximum code claims
        user_codes_claimed = len(db["users"][uid].get("code_history", []))
        max_codes = db["settings"]["max_codes_per_user"]
        if user_codes_claimed >= max_codes:
            return False
        rate_limit = 5  # Allow 5 redemption attempts per hour
    else:
        rate_limit = 10  # Default rate limit for other actions
    
    if len(db["user_limits"][uid][action]) >= rate_limit:
        return False
    
    # Add current timestamp
    db["user_limits"][uid][action].append(now)
    save_db(db)
    return True

# --- ADMIN ACTION LIMIT CHECK ---
async def check_admin_limit(uid, action="code_gen"):
    """Check admin action limits"""
    # Owner has unlimited access
    if is_owner(uid):
        return True
    
    db = load_db()
    uid = str(uid)
    today = datetime.now().strftime("%Y-%m-%d")
    
    if uid not in db["admin_actions"]:
        db["admin_actions"][uid] = {}
    
    if action not in db["admin_actions"][uid]:
        db["admin_actions"][uid][action] = {}
    
    if today not in db["admin_actions"][uid][action]:
        db["admin_actions"][uid][action][today] = 0
    
    # Check daily limit
    if action == "code_gen":
        daily_limit = db["settings"]["admin_code_gen_limit"]
        if db["admin_actions"][uid][action][today] >= daily_limit:
            return False
    
    # Increment counter
    db["admin_actions"][uid][action][today] += 1
    save_db(db)
    return True

# --- REDEEM CODE GENERATOR ---
def generate_redeem_code(days, name, limit=1, expires_in_days=30, category="General", scheduled_activate=None):
    """Generate a unique redeem code with custom parameters"""
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    
    db = load_db()
    
    # Ensure code is unique
    while code in db["redeem_codes"]:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    
    # Calculate expiration date
    created_at = datetime.now()
    expires_at = created_at + timedelta(days=expires_in_days)
    
    # Set activation date
    if scheduled_activate:
        activate_at = created_at + timedelta(days=scheduled_activate)
    else:
        activate_at = created_at
    
    # Validate days against max limit
    max_days = db["settings"]["max_days_per_code"]
    days = min(days, max_days)
    
    # Add code to database
    db["redeem_codes"][code] = {
        "name": name,
        "days": days,
        "limit": limit,  # Maximum number of times this code can be used
        "used": 0,  # How many times this code has been used
        "category": category,
        "created_at": created_at.isoformat(),
        "activate_at": activate_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "claimed_by": [],  # List of user IDs who claimed this code
        "claimed_at": [],  # List of timestamps when the code was claimed
        "active": scheduled_activate is None  # False if scheduled for future activation
    }
    
    # Update stats
    db["code_stats"]["total_generated"] += 1
    db["code_stats"]["total_days"] += days
    
    # Track daily generation
    today = created_at.strftime("%Y-%m-%d")
    if today not in db["code_stats"]["daily_generated"]:
        db["code_stats"]["daily_generated"][today] = 0
    db["code_stats"]["daily_generated"][today] += 1
    
    save_db(db)
    return code, expires_at

# --- CANCEL COMMAND ---
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current conversation"""
    await update.message.reply_text("❌ **Operation cancelled.**")
    return ConversationHandler.END

# --- START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = str(user.id)
    db = load_db()

    if uid not in db["users"]:
        ref = context.args[0] if context.args and context.args[0] in db["users"] else None
        db["users"][uid] = {
            "name": user.first_name, 
            "points": 1, 
            "clones": 0, 
            "plan": "Normal",
            "last_used": None,
            "code_history": [],  # Track codes used by this user
            "joined_channels": []  # Track which channels user has joined
        }
        if ref:
            db["users"][ref]["points"] += 1
            try: 
                await context.bot.send_message(ref, "🎁 **Referral Reward!** +1 Point.")
            except: 
                pass
        save_db(db)

    # Check if user has premium
    premium_status = "✅ Premium" if is_premium(uid) else "❌ Normal"
    if is_premium(uid):
        expiry_date = datetime.fromisoformat(db["users"][uid]["premium_expires"])
        premium_status += f" (Expires: {expiry_date.strftime('%Y-%m-%d')})"
    
    # Owner status
    owner_status = "👑 **OWNER**" if is_owner(uid) else ""

    # Clean UI Menu
    kb = [["🚀 Clone", "👤 Profile"], ["🏆 Leaderboard", "🔗 Refer"], ["🎫 Redeem Code", "ℹ️ Help"]]
    await update.message.reply_text(
        f"👋 **Hi {user.first_name}!**\n**Sticker Cloner Pro** is ready.\n\nStatus: {premium_status} {owner_status}\n\nUse the buttons below to start.",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True), 
        parse_mode="Markdown"
    )

    # Secret Admin Panel (Only for you)
    if user.id == ADMIN_ID:
        adm_kb = [
            [InlineKeyboardButton("📢 Broadcast", callback_data="adm_bc"), InlineKeyboardButton("📊 Stats", callback_data="adm_st")],
            [InlineKeyboardButton("🎫 Generate Code", callback_data="adm_gen"), InlineKeyboardButton("📈 Code Stats", callback_data="adm_code_stats")],
            [InlineKeyboardButton("📺 Manage Channels", callback_data="adm_channels"), InlineKeyboardButton("🎫 Code Templates", callback_data="adm_templates")],
            [InlineKeyboardButton("🔧 Advanced Settings", callback_data="adm_settings")]
        ]
        await update.message.reply_text("🛠 **ADMIN PANEL**", reply_markup=InlineKeyboardMarkup(adm_kb))

# --- CLONING HANDLERS ---
async def start_clone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    # Check if user needs to join channels
    if not is_owner(uid) and "clone" in load_db()["settings"]["force_join_for"]:
        if not await is_sub(context.bot, uid):
            channels = load_db()["settings"]["channels"]
            channel_links = "\n".join([f"• [{ch}](https://t.me/{ch})" for ch in channels])
            return await update.message.reply_text(
                f"❌ **Join our channels first to use the bot!**\n\n{channel_links}",
                parse_mode="Markdown"
            )
    
    # Set rate limit based on user plan
    rate_limit = RATE_LIMIT_OWNER if is_owner(uid) else (RATE_LIMIT_PREMIUM if is_premium(uid) else RATE_LIMIT_NORMAL)
    
    if not await check_rate_limit(uid, "clone"):
        return await update.message.reply_text(f"⏰ **Rate limit exceeded!** You can only create {rate_limit} packs per hour. Please try again later.")
    
    await update.message.reply_text("🔗 **Send the Sticker Pack Link:**\n\nType /cancel to cancel this operation.")
    return LNK

async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    link = update.message.text
    
    # Enhanced link validation
    if not re.match(r'https?://t\.me/addstickers/.*', link):
        await update.message.reply_text("❌ **Invalid Link!** Send a valid Telegram sticker link.\nExample: https://t.me/addstickers/YourPackName\n\nType /cancel to cancel this operation.")
        return LNK
    
    pack_name = link.split('/')[-1]
    context.user_data['old_pack'] = pack_name
    
    try:
        # Check if pack exists and get info
        sticker_set = await context.bot.get_sticker_set(pack_name)
        sticker_count = len(sticker_set.stickers)
        
        # Create keyboard with options
        keyboard = [
            [InlineKeyboardButton(f"Clone All ({sticker_count} stickers)", callback_data="clone_all")],
            [InlineKeyboardButton("Select Specific Stickers", callback_data="select_stickers")]
        ]
        
        await update.message.reply_text(
            f"📦 **Pack Found:** {sticker_set.title}\n"
            f"📊 **Stickers:** {sticker_count}\n\n"
            f"Choose cloning option:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return NME
    except Exception as e:
        await update.message.reply_text(f"❌ **Error:** {str(e)[:100]}\n\nPlease check the link and try again.\n\nType /cancel to cancel this operation.")
        return LNK

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text
    context.user_data['new_name'] = new_name
    context.user_data['clone_all'] = True
    
    await update.message.reply_text("⏳ **Preparing to clone all stickers...**\nThis may take a while for large packs.")
    return await process_clone(update, context)

async def process_clone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    old_pack = context.user_data['old_pack']
    new_name = context.user_data['new_name']
    uid = str(update.effective_user.id)
    clone_all = context.user_data.get('clone_all', True)
    
    status = await update.message.reply_text("⏳ **Cloning... please wait.**")

    try:
        old_set = await context.bot.get_sticker_set(old_pack)
        bot_un = (await context.bot.get_me()).username
        new_short = f"s_{random.randint(100,999)}_{uid}_by_{bot_un}"
        
        # Process all stickers or selected ones
        if clone_all:
            stickers_to_process = old_set.stickers
        else:
            # For now, use all stickers - in a full implementation, 
            # you'd have a selection interface
            stickers_to_process = old_set.stickers
        
        # Process stickers in batches to avoid timeouts
        all_stickers = []
        total_stickers = len(stickers_to_process)
        
        # Update status message
        await status.edit_text(f"⏳ **Cloning...** 0/{total_stickers} stickers processed")
        
        for i in range(0, total_stickers, BATCH_SIZE):
            batch = stickers_to_process[i:i+BATCH_SIZE]
            
            for j, sticker in enumerate(batch):
                # Determine sticker format
                if sticker.is_video:
                    sticker_format = "video"
                elif sticker.is_animated:
                    sticker_format = "animated"
                else:
                    sticker_format = "static"
                
                # Create InputSticker object
                try:
                    input_sticker = InputSticker(
                        sticker=sticker.file_id,
                        emoji_list=[sticker.emoji or "✨"],
                        format=sticker_format
                    )
                    all_stickers.append(input_sticker)
                except Exception as e:
                    print(f"Error processing sticker {i+j+1}: {e}")
                
                # Update progress every 10 stickers
                if (i + j) % 10 == 0:
                    try:
                        await status.edit_text(f"⏳ **Cloning...** {i+j}/{total_stickers} stickers processed")
                    except:
                        pass  # Ignore errors when updating status
        
        # Create the sticker set with all stickers
        try:
            # Create the sticker set with the first batch
            await context.bot.create_new_sticker_set(
                user_id=update.effective_user.id,  # Use user's ID instead of admin
                name=new_short,
                title=new_name,
                stickers=all_stickers[:50]  # First batch
            )
            
            # Add remaining stickers in batches
            if len(all_stickers) > 50:
                for i in range(50, len(all_stickers), 50):
                    batch = all_stickers[i:i+50]
                    await context.bot.add_sticker_to_set(
                        user_id=update.effective_user.id,
                        name=new_short,
                        sticker=batch[0]
                    )
                    
                    # Add the rest of the batch
                    for sticker in batch[1:]:
                        await context.bot.add_sticker_to_set(
                            user_id=update.effective_user.id,
                            name=new_short,
                            sticker=sticker
                        )
            
            # Update user stats
            db = load_db()
            db["users"][uid]["clones"] += 1
            db["users"][uid]["last_used"] = datetime.now().isoformat()
            save_db(db)

            await status.edit_text(
                f"✅ **Pack Created Successfully!**\n"
                f"📦 **Name:** {new_name}\n"
                f"🔗 **Link:** https://t.me/addstickers/{new_short}\n"
                f"📊 **Stickers:** {len(all_stickers)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Pack", url=f"https://t.me/addstickers/{new_short}")],
                    [InlineKeyboardButton("🔗 Share", url=f"https://t.me/share/url?url=https://t.me/addstickers/{new_short}&text={new_name}")]
                ])
            )
        except BadRequest as e:
            if "STICKERSET_INVALID" in str(e):
                # Try with admin ID if user ID fails
                await context.bot.create_new_sticker_set(
                    user_id=ADMIN_ID,
                    name=new_short,
                    title=new_name,
                    stickers=all_stickers[:50]
                )
                
                # Add remaining stickers
                if len(all_stickers) > 50:
                    for i in range(50, len(all_stickers), 50):
                        batch = all_stickers[i:i+50]
                        await context.bot.add_sticker_to_set(
                            user_id=ADMIN_ID,
                            name=new_short,
                            sticker=batch[0]
                        )
                        
                        for sticker in batch[1:]:
                            await context.bot.add_sticker_to_set(
                                user_id=ADMIN_ID,
                                name=new_short,
                                sticker=sticker
                            )
                
                await status.edit_text(
                    f"✅ **Pack Created Successfully!**\n"
                    f"📦 **Name:** {new_name}\n"
                    f"🔗 **Link:** https://t.me/addstickers/{new_short}\n"
                    f"📊 **Stickers:** {len(all_stickers)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("➕ Add Pack", url=f"https://t.me/addstickers/{new_short}")],
                        [InlineKeyboardButton("🔗 Share", url=f"https://t.me/share/url?url=https://t.me/addstickers/{new_short}&text={new_name}")]
                    ])
                )
            else:
                raise e
    except RetryAfter as e:
        retry_after = e.retry_after
        await status.edit_text(f"⏰ **Rate limited by Telegram.** Please wait {retry_after} seconds and try again.")
        return ConversationHandler.END
    except Exception as e:
        await status.edit_text(f"❌ **Error:** `{str(e)[:200]}`\n\nPlease try again or contact support.")
        return ConversationHandler.END
    
    return ConversationHandler.END

# --- REDEEM CODE HANDLERS ---
async def start_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    # Check if user needs to join channels
    if not is_owner(uid) and "redeem" in load_db()["settings"]["force_join_for"]:
        if not await is_sub(context.bot, uid):
            channels = load_db()["settings"]["channels"]
            channel_links = "\n".join([f"• [{ch}](https://t.me/{ch})" for ch in channels])
            return await update.message.reply_text(
                f"❌ **Join our channels first to use the bot!**\n\n{channel_links}",
                parse_mode="Markdown"
            )
    
    # Check rate limit
    if not await check_rate_limit(uid, "redeem"):
        return await update.message.reply_text("⏰ **Rate limit exceeded!** Please try again later.")
    
    await update.message.reply_text("🎫 **Enter your redeem code:**\n\nType /cancel to cancel this operation.")
    return REDEEM

async def process_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.upper().strip()
    uid = str(update.effective_user.id)
    db = load_db()
    
    # Check if code exists
    if code not in db["redeem_codes"]:
        await update.message.reply_text("❌ **Invalid code!** Please check and try again.\n\nType /cancel to cancel this operation.")
        return REDEEM
    
    code_data = db["redeem_codes"][code]
    
    # Check if code is active
    if not code_data["active"]:
        activate_at = datetime.fromisoformat(code_data["activate_at"])
        await update.message.reply_text(f"❌ **This code is not active yet!**\n\nActivates on: {activate_at.strftime('%Y-%m-%d %H:%M')}\n\nType /cancel to cancel this operation.")
        return ConversationHandler.END
    
    # Check if code has reached its usage limit
    if code_data["used"] >= code_data["limit"]:
        await update.message.reply_text(f"❌ **This code has reached its usage limit!**\n\nLimit: {code_data['limit']} uses\nUsed: {code_data['used']} times\n\nType /cancel to cancel this operation.")
        return ConversationHandler.END
    
    # Check if user has already claimed this code
    if uid in code_data["claimed_by"]:
        await update.message.reply_text("❌ **You have already used this code!**\n\nType /cancel to cancel this operation.")
        return ConversationHandler.END
    
    # Check if code is expired
    expires_at = datetime.fromisoformat(code_data["expires_at"])
    if expires_at < datetime.now():
        await update.message.reply_text(f"❌ **This code has expired!**\n\nExpired on: {expires_at.strftime('%Y-%m-%d')}\n\nType /cancel to cancel this operation.")
        return ConversationHandler.END
    
    # Check if user has reached their maximum code claims
    user_codes_claimed = len(db["users"][uid].get("code_history", []))
    max_codes = db["settings"]["max_codes_per_user"]
    if user_codes_claimed >= max_codes and not is_owner(uid):
        await update.message.reply_text(f"❌ **You have reached your maximum code claims!**\n\nMaximum: {max_codes} codes\nClaimed: {user_codes_claimed} codes\n\nType /cancel to cancel this operation.")
        return ConversationHandler.END
    
    # Check if user already has premium
    current_premium = is_premium(uid)
    
    # Update user's premium status
    if uid not in db["users"]:
        db["users"][uid] = {
            "name": update.effective_user.first_name,
            "points": 0,
            "clones": 0,
            "plan": "Normal",
            "last_used": None,
            "code_history": []
        }
    
    # Add code to user's history
    db["users"][uid]["code_history"].append({
        "code": code,
        "name": code_data["name"],
        "category": code_data["category"],
        "claimed_at": datetime.now().isoformat(),
        "days": code_data["days"]
    })
    
    # Calculate new expiry date
    if current_premium:
        current_expiry = datetime.fromisoformat(db["users"][uid]["premium_expires"])
        new_expiry = current_expiry + timedelta(days=code_data["days"])
    else:
        new_expiry = datetime.now() + timedelta(days=code_data["days"])
    
    # Update user's premium expiry
    db["users"][uid]["premium_expires"] = new_expiry.isoformat()
    db["users"][uid]["plan"] = "Premium"
    
    # Update code usage
    db["redeem_codes"][code]["claimed_by"].append(uid)
    db["redeem_codes"][code]["claimed_at"].append(datetime.now().isoformat())
    db["redeem_codes"][code]["used"] += 1
    
    # Update stats
    db["code_stats"]["total_claimed"] += 1
    
    # Track daily claims
    today = datetime.now().strftime("%Y-%m-%d")
    if today not in db["code_stats"]["daily_claimed"]:
        db["code_stats"]["daily_claimed"][today] = 0
    db["code_stats"]["daily_claimed"][today] += 1
    
    # Update unique users claimed if this is the first time this user claimed a code
    if len(db["users"][uid]["code_history"]) == 1:
        db["code_stats"]["unique_users_claimed"] += 1
    
    save_db(db)
    
    # Calculate remaining uses
    remaining_uses = code_data["limit"] - db["redeem_codes"][code]["used"]
    
    # Get rate limit based on user plan
    rate_limit = RATE_LIMIT_OWNER if is_owner(uid) else (RATE_LIMIT_PREMIUM if is_premium(uid) else RATE_LIMIT_NORMAL)
    
    await update.message.reply_text(
        f"✅ **Code redeemed successfully!**\n\n"
        f"🎫 **Code Name:** {code_data['name']}\n"
        f"🏷️ **Category:** {code_data['category']}\n"
        f"🎁 **Premium Days Added:** {code_data['days']}\n"
        f"📅 **New Expiry Date:** {new_expiry.strftime('%Y-%m-%d')}\n\n"
        f"📊 **Code Usage:** {db['redeem_codes'][code]['used']}/{code_data['limit']} uses\n"
        f"🔄 **Remaining Uses:** {remaining_uses}\n\n"
        f"Enjoy your premium benefits:\n"
        f"• {rate_limit} packs per hour\n"
        f"• Priority support\n"
        f"• Access to premium features"
    )
    
    return ConversationHandler.END

# --- ADMIN CODE GENERATION ---
async def start_gen_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Check admin limit
    if not await check_admin_limit(update.effective_user.id, "code_gen"):
        return await update.message.reply_text("⏰ **Daily code generation limit reached!** Please try again tomorrow.")
    
    # Show advanced form for code generation
    keyboard = [
        [InlineKeyboardButton("🎫 Single Use Code", callback_data="gen_single")],
        [InlineKeyboardButton("🎫 Multi-Use Code", callback_data="gen_multi")],
        [InlineKeyboardButton("🎫 Unlimited Use Code", callback_data="gen_unlimited")],
        [InlineKeyboardButton("📋 Use Template", callback_data="gen_template")],
        [InlineKeyboardButton("📦 Bulk Generate", callback_data="gen_bulk")],
        [InlineKeyboardButton("❌ Cancel", callback_data="gen_cancel")]
    ]
    
    await update.message.reply_text(
        "🎫 **REDEEM CODE GENERATOR**\n\n"
        "Select the type of code you want to generate:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return GEN_CODE_TYPE

async def gen_code_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "gen_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    elif query.data == "gen_template":
        # Show available templates
        db = load_db()
        templates = db.get("code_templates", {})
        
        if not templates:
            await query.message.reply_text("❌ **No templates available!** Please create templates first.")
            return ConversationHandler.END
        
        keyboard = []
        for template_id, template in templates.items():
            keyboard.append([InlineKeyboardButton(f"{template['name']} ({template['days']} days)", callback_data=f"template_{template_id}")])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="gen_cancel")])
        
        await query.message.reply_text(
            "📋 **Select a template:**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return TEMPLATE_SELECT
    elif query.data == "gen_bulk":
        await query.message.reply_text("📦 **Enter the number of codes to generate:**\n\nType /cancel to cancel this operation.")
        return BULK_GEN
    
    # Set the code limit based on selection
    if query.data == "gen_single":
        context.user_data['code_limit'] = 1
        await query.message.reply_text("🎫 **Enter a name for this single-use code:**\n\nType /cancel to cancel this operation.")
    elif query.data == "gen_multi":
        await query.message.reply_text("🎫 **Enter the usage limit for this multi-use code:**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_LIMIT
    elif query.data == "gen_unlimited":
        context.user_data['code_limit'] = 999  # Use a large number for "unlimited"
        await query.message.reply_text("🎫 **Enter a name for this unlimited-use code:**\n\nType /cancel to cancel this operation.")
    
    return GEN_CODE_NAME

async def gen_from_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "gen_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    
    # Extract template ID
    template_id = query.data.replace("template_", "")
    
    db = load_db()
    template = db["code_templates"].get(template_id)
    
    if not template:
        await query.message.reply_text("❌ **Template not found!**")
        return ConversationHandler.END
    
    # Generate code using template
    code, expires_at = generate_redeem_code(
        template["days"],
        template["name"],
        template.get("limit", 1),
        template.get("expires_in_days", 30),
        template.get("category", "General"),
        template.get("scheduled_activate", None)
    )
    
    # Format the limit text
    limit = template.get("limit", 1)
    if limit >= 999:
        limit_text = "Unlimited"
    else:
        limit_text = str(limit)
    
    await query.message.reply_text(
        f"✅ **Code generated from template!**\n\n"
        f"🎫 **Code Name:** {template['name']}\n"
        f"🔑 **Code:** `{code}`\n"
        f"📅 **Days:** {template['days']}\n"
        f"🏷️ **Category:** {template.get('category', 'General')}\n"
        f"🔄 **Usage Limit:** {limit_text}\n"
        f"🗓️ **Expires:** {expires_at.strftime('%Y-%m-%d')}\n\n"
        f"Share this code with users to grant them premium access.",
        parse_mode="Markdown"
    )
    
    return ConversationHandler.END

async def get_bulk_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        count = int(update.message.text)
        if count <= 0 or count > 100:  # Limit bulk generation to 100 codes
            await update.message.reply_text("❌ **Invalid number!** Please enter a number between 1 and 100.\n\nType /cancel to cancel this operation.")
            return BULK_GEN
        
        context.user_data['bulk_count'] = count
        await update.message.reply_text(f"📦 **Enter a base name for these {count} codes:**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_NAME
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number.\n\nType /cancel to cancel this operation.")
        return BULK_GEN

async def get_code_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text)
        if limit <= 1:
            await update.message.reply_text("❌ **For single-use codes, please use the 'Single Use Code' option.**\n\nEnter a usage limit greater than 1, or type /cancel to cancel this operation.")
            return GEN_CODE_LIMIT
        
        context.user_data['code_limit'] = limit
        await update.message.reply_text(f"🎫 **Enter a name for this {limit}-use code:**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_NAME
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_LIMIT

async def get_code_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("❌ **Name cannot be empty!** Please enter a name for this code.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_NAME
    
    context.user_data['code_name'] = name
    
    # Check if this is bulk generation
    if 'bulk_count' in context.user_data:
        await update.message.reply_text(f"🎫 **Enter number of days for '{name}':**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_DAYS
    
    # Show category selection
    db = load_db()
    categories = db["settings"]["code_categories"]
    
    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(category, callback_data=f"cat_{category}")])
    
    keyboard.append([InlineKeyboardButton("Custom", callback_data="cat_custom")])
    
    await update.message.reply_text(
        f"🎫 **Select category for '{name}':**",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return GEN_CODE_CATEGORY

async def get_code_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "cat_custom":
        await query.message.reply_text("🎫 **Enter a custom category:**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_CATEGORY
    
    # Extract category
    category = query.data.replace("cat_", "")
    context.user_data['code_category'] = category
    
    await query.message.reply_text(f"🎫 **Enter number of days for '{context.user_data['code_name']}':**\n\nType /cancel to cancel this operation.")
    return GEN_CODE_DAYS

async def get_custom_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    category = update.message.text.strip()
    if not category:
        await update.message.reply_text("❌ **Category cannot be empty!** Please enter a category.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_CATEGORY
    
    context.user_data['code_category'] = category
    await update.message.reply_text(f"🎫 **Enter number of days for '{context.user_data['code_name']}':**\n\nType /cancel to cancel this operation.")
    return GEN_CODE_DAYS

async def get_code_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text)
        if days <= 0:
            await update.message.reply_text("❌ **Invalid number!** Please enter a positive number of days.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_DAYS
        
        # Check against max days limit
        db = load_db()
        max_days = db["settings"]["max_days_per_code"]
        if days > max_days and not is_owner(update.effective_user.id):
            await update.message.reply_text(f"❌ **Maximum days per code is {max_days}!**\n\nPlease enter a smaller number or type /cancel to cancel this operation.")
            return GEN_CODE_DAYS
        
        context.user_data['code_days'] = days
        
        # Check if this is bulk generation
        if 'bulk_count' in context.user_data:
            # Ask for expiry days
            keyboard = [
                [InlineKeyboardButton("7 Days", callback_data="exp_7")],
                [InlineKeyboardButton("30 Days", callback_data="exp_30")],
                [InlineKeyboardButton("90 Days", callback_data="exp_90")],
                [InlineKeyboardButton("Custom", callback_data="exp_custom")]
            ]
            
            await update.message.reply_text(
                f"🎫 **Select expiry period for bulk codes:**",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            return GEN_CODE_EXPIRY
        
        # Ask for scheduled activation
        keyboard = [
            [InlineKeyboardButton("Activate Now", callback_data="act_now")],
            [InlineKeyboardButton("Schedule Activation", callback_data="act_schedule")],
            [InlineKeyboardButton("❌ Cancel", callback_data="gen_cancel")]
        ]
        
        await update.message.reply_text(
            f"🎫 **Select activation option for '{context.user_data['code_name']}':**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return GEN_CODE_EXPIRY
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number of days.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_DAYS

async def get_code_activation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "gen_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    elif query.data == "act_now":
        context.user_data['scheduled_activate'] = None
    elif query.data == "act_schedule":
        await query.message.reply_text("🎫 **Enter days until activation (0-365):**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY
    
    # Ask for expiry days
    keyboard = [
        [InlineKeyboardButton("7 Days", callback_data="exp_7")],
        [InlineKeyboardButton("30 Days", callback_data="exp_30")],
        [InlineKeyboardButton("90 Days", callback_data="exp_90")],
        [InlineKeyboardButton("Custom", callback_data="exp_custom")]
    ]
    
    await query.message.reply_text(
        f"🎫 **Select expiry period for '{context.user_data['code_name']}':**",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return GEN_CODE_EXPIRY

async def get_scheduled_activation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text)
        if days < 0 or days > 365:
            await update.message.reply_text("❌ **Invalid number!** Please enter a number between 0 and 365.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_EXPIRY
        
        context.user_data['scheduled_activate'] = days
        
        # Ask for expiry days
        keyboard = [
            [InlineKeyboardButton("7 Days", callback_data="exp_7")],
            [InlineKeyboardButton("30 Days", callback_data="exp_30")],
            [InlineKeyboardButton("90 Days", callback_data="exp_90")],
            [InlineKeyboardButton("Custom", callback_data="exp_custom")]
        ]
        
        await update.message.reply_text(
            f"🎫 **Select expiry period for '{context.user_data['code_name']}':**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return GEN_CODE_EXPIRY
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY

async def get_code_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "exp_7":
        expiry_days = 7
    elif query.data == "exp_30":
        expiry_days = 30
    elif query.data == "exp_90":
        expiry_days = 90
    elif query.data == "exp_custom":
        await query.message.reply_text("🎫 **Enter custom expiry period in days:**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY
    
    # Check if this is bulk generation
    if 'bulk_count' in context.user_data:
        # Generate bulk codes
        await generate_bulk_codes(update, context, expiry_days)
        return ConversationHandler.END
    
    # Generate the single code with all parameters
    name = context.user_data['code_name']
    days = context.user_data['code_days']
    limit = context.user_data['code_limit']
    category = context.user_data['code_category']
    scheduled_activate = context.user_data.get('scheduled_activate', None)
    
    code, expires_at = generate_redeem_code(days, name, limit, expiry_days, category, scheduled_activate)
    
    # Format the limit text
    if limit >= 999:
        limit_text = "Unlimited"
    else:
        limit_text = str(limit)
    
    # Format activation text
    if scheduled_activate is not None:
        activate_at = datetime.now() + timedelta(days=scheduled_activate)
        activation_text = f"🚀 **Activates on:** {activate_at.strftime('%Y-%m-%d')}\n"
    else:
        activation_text = ""
    
    await query.message.reply_text(
        f"✅ **Code generated successfully!**\n\n"
        f"🎫 **Code Name:** {name}\n"
        f"🔑 **Code:** `{code}`\n"
        f"📅 **Days:** {days}\n"
        f"🏷️ **Category:** {category}\n"
        f"🔄 **Usage Limit:** {limit_text}\n"
        f"🗓️ **Expires:** {expires_at.strftime('%Y-%m-%d')}\n"
        f"{activation_text}\n"
        f"Share this code with users to grant them premium access.",
        parse_mode="Markdown"
    )
    
    return ConversationHandler.END

async def get_custom_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        expiry_days = int(update.message.text)
        if expiry_days <= 0:
            await update.message.reply_text("❌ **Invalid number!** Please enter a positive number of days.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_EXPIRY
        
        # Check if this is bulk generation
        if 'bulk_count' in context.user_data:
            # Generate bulk codes
            await generate_bulk_codes(update, context, expiry_days)
            return ConversationHandler.END
        
        # Generate the single code with all parameters
        name = context.user_data['code_name']
        days = context.user_data['code_days']
        limit = context.user_data['code_limit']
        category = context.user_data['code_category']
        scheduled_activate = context.user_data.get('scheduled_activate', None)
        
        code, expires_at = generate_redeem_code(days, name, limit, expiry_days, category, scheduled_activate)
        
        # Format the limit text
        if limit >= 999:
            limit_text = "Unlimited"
        else:
            limit_text = str(limit)
        
        # Format activation text
        if scheduled_activate is not None:
            activate_at = datetime.now() + timedelta(days=scheduled_activate)
            activation_text = f"🚀 **Activates on:** {activate_at.strftime('%Y-%m-%d')}\n"
        else:
            activation_text = ""
        
        await update.message.reply_text(
            f"✅ **Code generated successfully!**\n\n"
            f"🎫 **Code Name:** {name}\n"
            f"🔑 **Code:** `{code}`\n"
            f"📅 **Days:** {days}\n"
            f"🏷️ **Category:** {category}\n"
            f"🔄 **Usage Limit:** {limit_text}\n"
            f"🗓️ **Expires:** {expires_at.strftime('%Y-%m-%d')}\n"
            f"{activation_text}\n"
            f"Share this code with users to grant them premium access.",
            parse_mode="Markdown"
        )
        
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number of days.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY

async def generate_bulk_codes(update: Update, context: ContextTypes.DEFAULT_TYPE, expiry_days):
    count = context.user_data['bulk_count']
    name = context.user_data['code_name']
    days = context.user_data['code_days']
    
    status = await update.message.reply_text(f"📦 **Generating {count} codes...** 0%")
    
    codes = []
    for i in range(count):
        # Generate code with base name and number
        code_name = f"{name} #{i+1}"
        code, expires_at = generate_redeem_code(days, code_name, 1, expiry_days, "Bulk", None)
        codes.append((code, code_name, expires_at))
        
        # Update progress every 10 codes
        if (i + 1) % 10 == 0:
            progress = int((i + 1) / count * 100)
            try:
                await status.edit_text(f"📦 **Generating {count} codes...** {progress}%")
            except:
                pass
    
    # Create a text file with all codes
    codes_text = f"Generated {count} codes on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    codes_text += f"Base Name: {name}\n"
    codes_text += f"Days: {days}\n"
    codes_text += f"Expires in: {expiry_days} days\n\n"
    
    for code, code_name, expires_at in codes:
        codes_text += f"{code_name}: {code} (Expires: {expires_at.strftime('%Y-%m-%d')})\n"
    
    # Save to file
    filename = f"codes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, "w") as f:
        f.write(codes_text)
    
    # Send the file
    await status.edit_text(f"✅ **Generated {count} codes successfully!**")
    await update.message.reply_document(
        document=open(filename, "rb"),
        caption=f"📦 **Bulk Code Generation Complete**\n\n"
                f"Total Codes: {count}\n"
                f"Base Name: {name}\n"
                f"Days: {days}\n"
                f"Expires in: {expiry_days} days"
    )
    
    # Clean up the file
    os.remove(filename)

# --- CHANNEL MANAGEMENT ---
async def manage_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    db = load_db()
    channels = db["settings"]["channels"]
    force_join_for = db["settings"]["force_join_for"]
    
    # Create channel list
    channel_list = ""
    if channels:
        channel_list = "\n".join([f"• {ch}" for ch in channels])
    else:
        channel_list = "No channels added yet."
    
    # Create force join options
    force_options = ""
    if "clone" in force_join_for:
        force_options += "✅ Clone\n"
    if "redeem" in force_join_for:
        force_options += "✅ Redeem\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Channel", callback_data="channel_add")],
        [InlineKeyboardButton("➖ Remove Channel", callback_data="channel_remove")],
        [InlineKeyboardButton("⚙️ Force Join Settings", callback_data="channel_force")],
        [InlineKeyboardButton("❌ Cancel", callback_data="channel_cancel")]
    ]
    
    await query.message.reply_text(
        f"📺 **CHANNEL MANAGEMENT**\n\n"
        f"**Current Channels:**\n{channel_list}\n\n"
        f"**Force Join For:**\n{force_options}\n\n"
        f"Select an action:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return MANAGE_CHANNELS

async def channel_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "channel_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    elif query.data == "channel_add":
        await query.message.reply_text("➕ **Enter channel username (without @):**\n\nType /cancel to cancel this operation.")
        return MANAGE_CHANNELS
    elif query.data == "channel_remove":
        db = load_db()
        channels = db["settings"]["channels"]
        
        if not channels:
            await query.message.reply_text("❌ **No channels to remove!**")
            return MANAGE_CHANNELS
        
        keyboard = []
        for channel in channels:
            keyboard.append([InlineKeyboardButton(channel, callback_data=f"remove_{channel}")])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="channel_cancel")])
        
        await query.message.reply_text(
            "➖ **Select a channel to remove:**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return MANAGE_CHANNELS
    elif query.data == "channel_force":
        db = load_db()
        force_join_for = db["settings"]["force_join_for"]
        
        # Create keyboard with current state
        keyboard = []
        
        # Clone option
        if "clone" in force_join_for:
            keyboard.append([InlineKeyboardButton("❌ Disable Clone Force Join", callback_data="unforce_clone")])
        else:
            keyboard.append([InlineKeyboardButton("✅ Enable Clone Force Join", callback_data="force_clone")])
        
        # Redeem option
        if "redeem" in force_join_for:
            keyboard.append([InlineKeyboardButton("❌ Disable Redeem Force Join", callback_data="unforce_redeem")])
        else:
            keyboard.append([InlineKeyboardButton("✅ Enable Redeem Force Join", callback_data="force_redeem")])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="channel_cancel")])
        
        await query.message.reply_text(
            "⚙️ **Select actions that require channel join:**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return MANAGE_CHANNELS
    elif query.data.startswith("remove_"):
        channel = query.data.replace("remove_", "")
        
        db = load_db()
        if channel in db["settings"]["channels"]:
            db["settings"]["channels"].remove(channel)
            save_db(db)
            await query.message.reply_text(f"✅ **Removed channel:** {channel}")
        else:
            await query.message.reply_text(f"❌ **Channel not found:** {channel}")
        
        return MANAGE_CHANNELS
    elif query.data.startswith("force_") or query.data.startswith("unforce_"):
        action = query.data.split("_")[1]
        
        db = load_db()
        force_join_for = db["settings"]["force_join_for"]
        
        if query.data.startswith("force_"):
            if action not in force_join_for:
                force_join_for.append(action)
                await query.message.reply_text(f"✅ **Enabled force join for:** {action}")
            else:
                await query.message.reply_text(f"⚠️ **Force join already enabled for:** {action}")
        else:
            if action in force_join_for:
                force_join_for.remove(action)
                await query.message.reply_text(f"✅ **Disabled force join for:** {action}")
            else:
                await query.message.reply_text(f"⚠️ **Force join already disabled for:** {action}")
        
        db["settings"]["force_join_for"] = force_join_for
        save_db(db)
        
        return MANAGE_CHANNELS

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel = update.message.text.strip().lstrip('@')
    
    if not channel:
        await update.message.reply_text("❌ **Invalid channel!** Please enter a valid channel username.\n\nType /cancel to cancel this operation.")
        return MANAGE_CHANNELS
    
    # Check if channel exists
    try:
        chat = await context.bot.get_chat(channel)
        if chat.type != "channel":
            await update.message.reply_text("❌ **This is not a channel!** Please enter a valid channel username.\n\nType /cancel to cancel this operation.")
            return MANAGE_CHANNELS
    except Exception as e:
        await update.message.reply_text(f"❌ **Error accessing channel:** {str(e)}\n\nMake sure the bot is an admin in the channel.\n\nType /cancel to cancel this operation.")
        return MANAGE_CHANNELS
    
    # Add channel to database
    db = load_db()
    if channel not in db["settings"]["channels"]:
        db["settings"]["channels"].append(channel)
        save_db(db)
        await update.message.reply_text(f"✅ **Added channel:** @{channel}")
    else:
        await update.message.reply_text(f"⚠️ **Channel already exists:** @{channel}")
    
    return ConversationHandler.END

# --- CODE TEMPLATES ---
async def manage_templates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    db = load_db()
    templates = db.get("code_templates", {})
    
    if not templates:
        await query.message.reply_text("❌ **No templates available!** Use /createtemplate to create one.")
        return ConversationHandler.END
    
    keyboard = []
    for template_id, template in templates.items():
        keyboard.append([InlineKeyboardButton(f"{template['name']} ({template['days']} days)", callback_data=f"view_template_{template_id}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="template_cancel")])
    
    await query.message.reply_text(
        "📋 **CODE TEMPLATES**\n\n"
        "Select a template to view or edit:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return GEN_CODE_NAME

async def view_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "template_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    
    # Extract template ID
    template_id = query.data.replace("view_template_", "")
    
    db = load_db()
    template = db["code_templates"].get(template_id)
    
    if not template:
        await query.message.reply_text("❌ **Template not found!**")
        return ConversationHandler.END
    
    # Format the limit text
    limit = template.get("limit", 1)
    if limit >= 999:
        limit_text = "Unlimited"
    else:
        limit_text = str(limit)
    
    keyboard = [
        [InlineKeyboardButton("🗑️ Delete Template", callback_data=f"delete_template_{template_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="template_cancel")]
    ]
    
    await query.message.reply_text(
        f"📋 **TEMPLATE: {template['name']}**\n\n"
        f"📅 **Days:** {template['days']}\n"
        f"🏷️ **Category:** {template.get('category', 'General')}\n"
        f"🔄 **Usage Limit:** {limit_text}\n"
        f"🗓️ **Expires In:** {template.get('expires_in_days', 30)} days\n"
        f"🚀 **Scheduled Activation:** {'Yes' if template.get('scheduled_activate') else 'No'}\n\n",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return GEN_CODE_NAME

async def delete_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "template_cancel":
        await query.message.reply_text("❌ **Operation cancelled.**")
        return ConversationHandler.END
    
    # Extract template ID
    template_id = query.data.replace("delete_template_", "")
    
    db = load_db()
    if template_id in db["code_templates"]:
        template_name = db["code_templates"][template_id]["name"]
        del db["code_templates"][template_id]
        save_db(db)
        await query.message.reply_text(f"✅ **Deleted template:** {template_name}")
    else:
        await query.message.reply_text(f"❌ **Template not found!**")
    
    return ConversationHandler.END

# --- CREATE TEMPLATE COMMAND ---
async def create_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Start template creation conversation
    await update.message.reply_text("📋 **Enter a name for this template:**\n\nType /cancel to cancel this operation.")
    return GEN_CODE_NAME

async def get_template_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("❌ **Name cannot be empty!** Please enter a name for this template.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_NAME
    
    context.user_data['template_name'] = name
    await update.message.reply_text(f"📋 **Enter number of days for '{name}':**\n\nType /cancel to cancel this operation.")
    return GEN_CODE_DAYS

async def get_template_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text)
        if days <= 0:
            await update.message.reply_text("❌ **Invalid number!** Please enter a positive number of days.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_DAYS
        
        context.user_data['template_days'] = days
        await update.message.reply_text(f"📋 **Enter usage limit for '{context.user_data['template_name']}':**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_LIMIT
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number of days.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_DAYS

async def get_template_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text)
        if limit <= 0:
            await update.message.reply_text("❌ **Invalid number!** Please enter a positive number for the limit.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_LIMIT
        
        context.user_data['template_limit'] = limit
        await update.message.reply_text(f"📋 **Enter expiry period in days for '{context.user_data['template_name']}':**\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_LIMIT

async def get_template_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        expiry_days = int(update.message.text)
        if expiry_days <= 0:
            await update.message.reply_text("❌ **Invalid number!** Please enter a positive number of days.\n\nType /cancel to cancel this operation.")
            return GEN_CODE_EXPIRY
        
        # Generate a unique template ID
        template_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        
        db = load_db()
        if "code_templates" not in db:
            db["code_templates"] = {}
        
        # Add template to database
        db["code_templates"][template_id] = {
            "name": context.user_data['template_name'],
            "days": context.user_data['template_days'],
            "limit": context.user_data['template_limit'],
            "expires_in_days": expiry_days,
            "category": "Template",
            "created_at": datetime.now().isoformat()
        }
        
        save_db(db)
        
        await update.message.reply_text(
            f"✅ **Template created successfully!**\n\n"
            f"📋 **Name:** {context.user_data['template_name']}\n"
            f"📅 **Days:** {context.user_data['template_days']}\n"
            f"🔄 **Usage Limit:** {context.user_data['template_limit']}\n"
            f"🗓️ **Expires In:** {expiry_days} days\n\n"
            f"You can now use this template to generate codes quickly."
        )
        
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ **Invalid input!** Please enter a valid number of days.\n\nType /cancel to cancel this operation.")
        return GEN_CODE_EXPIRY

# --- CALLBACK HANDLERS ---
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "clone_all":
        await query.message.reply_text("📝 **Enter the New Name for your pack:**\n\nType /cancel to cancel this operation.")
        context.user_data['clone_all'] = True
        return NME
    elif query.data == "select_stickers":
        # In a full implementation, you'd show a sticker selection interface
        # For now, we'll just proceed with all stickers
        await query.message.reply_text("📝 **Enter the New Name for your pack:**\n(For now, all stickers will be cloned)\n\nType /cancel to cancel this operation.")
        context.user_data['clone_all'] = True
        return NME
    elif query.data == "adm_bc":
        await query.message.reply_text("📝 **Send message to broadcast:**\n\nType /cancel to cancel this operation.")
        return BDC
    elif query.data == "adm_st":
        db = load_db()
        total_users = len(db['users'])
        total_clones = sum(u['clones'] for u in db['users'].values())
        premium_users = sum(1 for u in db['users'].values() if is_premium(u))
        
        stats_text = (
            f"📊 **STATISTICS**\n\n"
            f"👥 **Total Users:** {total_users}\n"
            f"👑 **Premium Users:** {premium_users}\n"
            f"📦 **Total Clones:** {total_clones}\n"
            f"📈 **Average Clones/User:** {total_clones/total_users:.2f}\n"
        )
        
        await query.message.reply_text(stats_text, parse_mode="Markdown")
    elif query.data == "adm_gen":
        # Start the advanced code generation form
        keyboard = [
            [InlineKeyboardButton("🎫 Single Use Code", callback_data="gen_single")],
            [InlineKeyboardButton("🎫 Multi-Use Code", callback_data="gen_multi")],
            [InlineKeyboardButton("🎫 Unlimited Use Code", callback_data="gen_unlimited")],
            [InlineKeyboardButton("📋 Use Template", callback_data="gen_template")],
            [InlineKeyboardButton("📦 Bulk Generate", callback_data="gen_bulk")],
            [InlineKeyboardButton("❌ Cancel", callback_data="gen_cancel")]
        ]
        
        await query.message.reply_text(
            "🎫 **REDEEM CODE GENERATOR**\n\n"
            "Select the type of code you want to generate:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return GEN_CODE_TYPE
    elif query.data == "adm_code_stats":
        db = load_db()
        stats = db["code_stats"]
        
        # Calculate active codes
        active_codes = 0
        expired_codes = 0
        claimed_codes = 0
        total_uses = 0
        total_limits = 0
        
        for code, data in db["redeem_codes"].items():
            expires_at = datetime.fromisoformat(data["expires_at"])
            if expires_at < datetime.now():
                expired_codes += 1
            else:
                active_codes += 1
            
            claimed_codes += data["used"]
            total_uses += data["used"]
            total_limits += data["limit"]
        
        # Calculate usage percentage
        usage_percentage = (total_uses / total_limits * 100) if total_limits > 0 else 0
        
        # Get today's stats
        today = datetime.now().strftime("%Y-%m-%d")
        today_generated = stats["daily_generated"].get(today, 0)
        today_claimed = stats["daily_claimed"].get(today, 0)
        
        stats_text = (
            f"📈 **REDEEM CODE STATISTICS**\n\n"
            f"🎫 **Total Generated:** {stats['total_generated']}\n"
            f"✅ **Total Claims:** {stats['total_claimed']}\n"
            f"👥 **Unique Users Claimed:** {stats['unique_users_claimed']}\n"
            f"📅 **Total Days Distributed:** {stats['total_days']}\n"
            f"🟢 **Active Codes:** {active_codes}\n"
            f"🔴 **Expired Codes:** {expired_codes}\n"
            f"📊 **Usage Rate:** {usage_percentage:.1f}% ({total_uses}/{total_limits})\n\n"
            f"📅 **Today's Stats:**\n"
            f"• Generated: {today_generated}\n"
            f"• Claimed: {today_claimed}"
        )
        
        await query.message.reply_text(stats_text, parse_mode="Markdown")
    elif query.data == "adm_channels":
        return await manage_channels(update, context)
    elif query.data == "adm_templates":
        return await manage_templates(update, context)
    elif query.data == "adm_settings":
        db = load_db()
        settings = db["settings"]
        
        settings_text = (
            "⚙️ **BOT SETTINGS**\n\n"
            f"• Normal User Rate Limit: {RATE_LIMIT_NORMAL} packs/hour\n"
            f"• Premium User Rate Limit: {RATE_LIMIT_PREMIUM} packs/hour\n"
            f"• Max Stickers/Pack: {MAX_STICKERS_PER_PACK}\n"
            f"• Batch Size: {BATCH_SIZE}\n\n"
            f"🎫 **Code Settings:**\n"
            f"• Max Days Per Code: {settings['max_days_per_code']}\n"
            f"• Max Codes Per User: {settings['max_codes_per_user']}\n"
            f"• Admin Code Gen Limit: {settings['admin_code_gen_limit']}/day\n\n"
            f"📺 **Channel Settings:**\n"
            f"• Force Join For: {', '.join(settings['force_join_for'])}\n"
            f"• Total Channels: {len(settings['channels'])}\n\n"
            f"To change these settings, edit the source code or use admin commands."
        )
        
        await query.message.reply_text(settings_text, parse_mode="Markdown")
    elif query.data.startswith("gen_"):
        return await gen_code_type(update, context)
    elif query.data.startswith("exp_"):
        return await get_code_expiry(update, context)
    elif query.data.startswith("cat_"):
        return await get_code_category(update, context)
    elif query.data.startswith("act_"):
        return await get_code_activation(update, context)
    elif query.data.startswith("channel_"):
        return await channel_action(update, context)
    elif query.data.startswith("template_"):
        return await view_template(update, context)
    elif query.data.startswith("delete_template_"):
        return await delete_template(update, context)

# --- GENERAL HANDLERS ---
async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    uid = str(update.effective_user.id)
    db = load_db()

    if text == "👤 Profile":
        u = db["users"][uid]
        last_used = u.get("last_used", "Never")
        if last_used != "Never":
            try:
                last_used = datetime.fromisoformat(last_used).strftime("%Y-%m-%d %H:%M")
            except:
                pass
        
        # Premium status
        premium_status = "❌ Normal"
        if is_premium(uid):
            expiry_date = datetime.fromisoformat(db["users"][uid]["premium_expires"])
            premium_status = f"✅ Premium (Expires: {expiry_date.strftime('%Y-%m-%d')})"
        
        # Owner status
        owner_status = "👑 **OWNER**" if is_owner(uid) else ""
        
        # Rate limit based on plan
        rate_limit = RATE_LIMIT_OWNER if is_owner(uid) else (RATE_LIMIT_PREMIUM if is_premium(uid) else RATE_LIMIT_NORMAL)
        
        # Show code history if any
        code_history = ""
        if u.get("code_history"):
            code_history = "\n🎫 **Recent Codes:**\n"
            for code in u["code_history"][-3:]:  # Show last 3 codes
                claimed_at = datetime.fromisoformat(code["claimed_at"]).strftime("%Y-%m-%d")
                code_history += f"• {code['name']} ({code['days']} days) - {claimed_at}\n"
        
        await update.message.reply_text(
            f"👤 **USER PROFILE** {owner_status}\n"
            f"━━━━━━━━━━━━\n"
            f"💰 Points: `{u['points']}`\n"
            f"📦 Clones: `{u['clones']}`\n"
            f"⭐ Plan: **{premium_status}**\n"
            f"🕐 Last Used: {last_used}\n"
            f"📊 Rate Limit: {rate_limit} packs/hour"
            f"{code_history}",
            parse_mode="Markdown"
        )
    
    elif text == "🏆 Leaderboard":
        top = sorted(db["users"].items(), key=lambda x: x[1]['points'], reverse=True)[:5]
        msg = "🏆 **TOP REFERRERS**\n"
        for i, (id, d) in enumerate(top, 1): 
            msg += f"{i}. {d['name']} — `{d['points']} pts`\n"
        await update.message.reply_text(msg, parse_mode="Markdown")

    elif text == "🔗 Refer":
        bot_un = (await context.bot.get_me()).username
        await update.message.reply_text(
            f"🔗 **Your Referral Link:**\n`https://t.me/{bot_un}?start={uid}`\n\n"
            f"Share this link and get 1 point for each referral!",
            parse_mode="Markdown"
        )
    
    elif text == "🎫 Redeem Code":
        return await start_redeem(update, context)
    
    elif text == "ℹ️ Help":
        help_text = (
            "📖 **HELP**\n\n"
            "🚀 **How to clone stickers:**\n"
            "1. Click '🚀 Clone'\n"
            "2. Send the sticker pack link\n"
            "3. Choose to clone all or select specific stickers\n"
            "4. Enter a name for your new pack\n"
            "5. Wait for the bot to finish cloning\n\n"
            "🎫 **Premium Features:**\n"
            "• Higher rate limits\n"
            "• Priority support\n"
            "• Access to premium features\n\n"
            "📊 **Limits:**\n"
            f"• Normal: {RATE_LIMIT_NORMAL} packs per hour\n"
            f"• Premium: {RATE_LIMIT_PREMIUM} packs per hour\n"
            f"• Maximum {MAX_STICKERS_PER_PACK} stickers per pack\n\n"
            "🔗 **Referral System:**\n"
            "Share your referral link and get 1 point for each new user.\n\n"
            "💡 **Tip:** You can use /cancel to exit any operation."
        )
        await update.message.reply_text(help_text, parse_mode="Markdown")

# --- ADMIN ACTIONS ---
async def do_bc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = load_db()
    message_text = update.message.text
    success_count = 0
    fail_count = 0
    
    status = await update.message.reply_text("📢 **Broadcasting...** 0%")
    
    total_users = len(db["users"])
    
    for i, (uid, user_data) in enumerate(db["users"].items()):
        try:
            await context.bot.send_message(
                int(uid), 
                f"📢 **NOTIFICATION**\n\n{message_text}",
                parse_mode="Markdown"
            )
            success_count += 1
        except Exception as e:
            print(f"Failed to send to {uid}: {e}")
            fail_count += 1
        
        # Update progress every 10 users
        if i % 10 == 0:
            progress = int((i / total_users) * 100)
            try:
                await status.edit_text(f"📢 **Broadcasting...** {progress}%")
            except:
                pass
    
    await status.edit_text(
        f"✅ **Broadcast Finished.**\n"
        f"📊 **Sent:** {success_count}\n"
        f"❌ **Failed:** {fail_count}"
    )
    return ConversationHandler.END

# --- ERROR HANDLER ---
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger = context.bot.logger
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    
    # Try to notify user
    try:
        if update.message:
            await update.message.reply_text(
                "❌ **An error occurred.** Please try again later or contact support.",
                parse_mode="Markdown"
            )
    except:
        pass

# --- MAIN ---
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Priority 1: Conversations (Cloning, Admin Broadcast, Redeem Code, Generate Code)
    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^🚀 Clone$"), start_clone),
            MessageHandler(filters.Regex("^🎫 Redeem Code$"), start_redeem),
            CallbackQueryHandler(handle_callback, pattern="^(clone_all|select_stickers|adm_)")
        ],
        states={
            LNK: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_link)],
            NME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_name)],
            BDC: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_bc)],
            REDEEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_redeem)],
            GEN_CODE_TYPE: [CallbackQueryHandler(gen_code_type, pattern="^gen_")],
            GEN_CODE_NAME: [
                CallbackQueryHandler(gen_from_template, pattern="^template_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_code_name)
            ],
            GEN_CODE_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_code_days)],
            GEN_CODE_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_code_limit)],
            GEN_CODE_EXPIRY: [
                CallbackQueryHandler(get_code_expiry, pattern="^exp_"),
                CallbackQueryHandler(get_code_activation, pattern="^act_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_custom_expiry)
            ],
            GEN_CODE_CATEGORY: [
                CallbackQueryHandler(get_code_category, pattern="^cat_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_custom_category)
            ],
            MANAGE_CHANNELS: [
                CallbackQueryHandler(channel_action, pattern="^channel_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_channel)
            ],
            BULK_GEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_bulk_count)],
            TEMPLATE_SELECT: [CallbackQueryHandler(gen_from_template, pattern="^template_")]
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        per_chat=True
    )
    
    # Template creation conversation
    template_conv = ConversationHandler(
        entry_points=[CommandHandler("createtemplate", create_template)],
        states={
            GEN_CODE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_template_name)],
            GEN_CODE_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_template_days)],
            GEN_CODE_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_template_limit)],
            GEN_CODE_EXPIRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_template_expiry)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_chat=True
    )

    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)  # Conv handler MUST be before general MessageHandler
    app.add_handler(template_conv)
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))
    
    # Add error handler
    app.add_error_handler(error_handler)
    
    print("Bot is alive and FAST!")
    app.run_polling()

if __name__ == "__main__":
    main()
