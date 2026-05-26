
# ================================================================================
# KeralaCaptain Bot - Pure Streaming Engine V4.4
# ================================================================================
#
#   NEW IN V4.4 — ANTI-DOWNLOADER SECURITY UPGRADE:
#
#   FEATURE 1: /watch Route — serves player.html via aiohttp_jinja2.
#     - GET /watch?id=<post_id>  renders player.html template with video_id context.
#
#   FEATURE 2: 60-Second Rolling Token API
#     - GET /api/get_token?video_id=<id>
#     - Returns a stateless HMAC-SHA256 token valid for exactly 60 seconds.
#     - Secret key = BOT_TOKEN (never leaves the server).
#     - Token format (base64url): "<video_id>:<unix_timestamp>:<hmac_hex>"
#
#   FEATURE 3: Token Verification on /stream/{message_id}
#     - All stream requests now require ?token=<xyz>.
#     - Token is verified: correct HMAC signature AND age ≤ 60 seconds.
#     - Missing / invalid / expired token → 403 Forbidden immediately.
#     - Download managers holding a stale URL fail after exactly 60 seconds.
#
#   FEATURE 4: Dynamic Chunk Throttling (Speed Control)
#     - First 10 seconds (or after a fresh Range seek): full 1 MB chunks, no delay.
#     - After burst period: asyncio.sleep(0.5) injected between chunks.
#     - This keeps real viewers buffering normally while destroying bulk-download speed.
#     - yield_file() chunk_size stays exactly 1 MB — no Pyrogram offset math changes.
#     - Every new Range request resets the burst timer (seamless seek recovery).
#
#   COMPLETELY UNCHANGED:
#     - All bandwidth tracking & auto-kill (85 GB warning, 90 GB kill).
#     - MongoDB collections, flush logic, and lifetime stats.
#     - Referer protection (still applied before token check).
#     - ByteStreamer class, yield_file(), FileReferenceExpired refresh logic.
#     - Multi-client load balancing, admin panel, restart/kill handlers.
#     - chunk_size = 1024 * 1024 (1 MB).
#
#   NEW DEPENDENCIES (add to requirements.txt):
#     pip install aiohttp-jinja2 jinja2
#     (hmac and hashlib are stdlib — no install needed)
#
# ================================================================================

import os
import time
import hmac
import base64
import hashlib
import signal
import asyncio
import logging
import aiohttp
import sys
import psutil
import aiohttp_jinja2
import jinja2
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait, AuthBytesInvalid,
    FileReferenceExpired
)
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation

# Load .env file
load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s - %(levelname)s] - %(message)s'
)
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

# Bot process start time (used for uptime display)
start_time = time.time()


# ================================================================================
# CONFIGURATION
# ================================================================================

class Config:
    API_ID           = int(os.environ.get("API_ID", 0))
    API_HASH         = os.environ.get("API_HASH", "")
    BOT_TOKEN        = os.environ.get("BOT_TOKEN", "")
    ADMIN_IDS        = list(int(x) for x in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    MONGO_URI        = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID   = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL       = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT             = int(os.environ.get("PORT", 8080))
    PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU        = 'DYNO' in os.environ

    # ── Bandwidth Thresholds ──────────────────────────────────────────────────
    # Send admin warning Telegram message at 85 GB
    BANDWIDTH_WARNING_BYTES = 85 * 1024 * 1024 * 1024
    # Automatically trigger Dead Mode (stop all streams) at 90 GB
    BANDWIDTH_KILL_BYTES    = 90 * 1024 * 1024 * 1024
    # Flush the in-memory counter to MongoDB every 500 MB to reduce DB writes
    BANDWIDTH_FLUSH_EVERY   = 500 * 1024 * 1024

    # ── Streaming Throttle ────────────────────────────────────────────────────
    # Seconds of full-speed "burst" allowed at the start of every new connection
    # (or after a seek). After this, a sleep is injected between chunks.
    BURST_DURATION_SECS  = 10
    # asyncio.sleep() inserted between chunks AFTER the burst phase.
    # 0.5 s ≈ 2 MB/s sustained rate — enough for 1080p, deadly for bulk downloads.
    THROTTLE_SLEEP_SECS  = 0.5


# ── Validate required env vars ───────────────────────────────────────────────
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical(
        "FATAL: One or more required env vars are missing: "
        "API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, STREAM_URL, ADMIN_IDS"
    )
    exit(1)

# Global dynamic protected domain (loaded from DB at startup; changeable by admin)
CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN


# ================================================================================
# HELPER FUNCTIONS
# ================================================================================

async def encode(string: str) -> str:
    """Base64-encodes a string for use in stream URLs."""
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")


async def decode(base64_string: str) -> str:
    """Decodes a base64-encoded stream URL string."""
    base64_string = base64_string.strip("=")
    base64_bytes  = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes  = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")


def humanbytes(size) -> str:
    """Converts a byte count into a human-readable string (KB, MB, GB, TB)."""
    if not size:
        return "0 B"
    power        = 1024
    n            = 0
    power_labels = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n    += 1
    return f"{round(size, 2)} {power_labels[n]}B"


def get_readable_time(seconds: int) -> str:
    """Converts seconds into a human-readable duration string (e.g. 1d 2h 3m 4s)."""
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days:
        result += f"{days}d "
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours:
        result += f"{hours}h "
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes:
        result += f"{minutes}m "
    result += f"{int(seconds)}s"
    return result


# ================================================================================
# TOKEN SYSTEM (FEATURE 2 & 3) — HMAC-SHA256, 60-second validity
# ================================================================================
# Token format (after base64url decoding): "<video_id>:<unix_timestamp>:<hmac_hex>"
# Secret key = BOT_TOKEN (never sent to clients).
#
# Security properties:
#   - Stateless: no DB lookup needed to verify — pure crypto.
#   - Tamper-proof: any bit-flip in video_id or timestamp makes the HMAC invalid.
#   - Time-limited: tokens older than TOKEN_MAX_AGE_SECS return 403.
#   - Download-manager killer: HLS.js xhrSetup injects the rolling token into every
#     segment request. An old URL captured by 1DM/IDM stops working within 60 s.

TOKEN_MAX_AGE_SECS = 60  # Hard expiry — matches the 45-second refresh interval in player.html


def generate_stream_token(video_id: str) -> str:
    """
    Generates a fresh HMAC-SHA256 token for video_id, valid for TOKEN_MAX_AGE_SECS.
    Called by /api/get_token.
    """
    timestamp = int(time.time())
    message   = f"{video_id}:{timestamp}"
    sig       = hmac.new(
        Config.BOT_TOKEN.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()
    raw_token = f"{message}:{sig}"
    # base64url-encode so it's safe in URL query strings
    return base64.urlsafe_b64encode(raw_token.encode()).decode().rstrip("=")


def verify_stream_token(token: str) -> bool:
    """
    Verifies a token:
      1. Decodes base64url.
      2. Splits into (video_id, timestamp, sig).
      3. Recomputes expected HMAC and compares using constant-time compare_digest.
      4. Checks that the token is no older than TOKEN_MAX_AGE_SECS.

    Returns True only if ALL checks pass.
    """
    try:
        # Re-pad and decode
        padded  = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode()).decode()

        # Expected format: "<video_id>:<timestamp>:<64-char-hex>"
        # rsplit with maxsplit=2 handles video_ids that might contain ':'
        parts = decoded.rsplit(":", 2)
        if len(parts) != 3:
            return False

        video_id, timestamp_str, received_sig = parts

        # Timing attack: reconstruct and compare in constant time
        message      = f"{video_id}:{timestamp_str}"
        expected_sig = hmac.new(
            Config.BOT_TOKEN.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, received_sig):
            return False

        # Check token age
        token_age = time.time() - int(timestamp_str)
        if token_age > TOKEN_MAX_AGE_SECS or token_age < 0:
            return False

        return True

    except Exception:
        return False


# ================================================================================
# DATABASE SETUP
# ================================================================================

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db        = db_client['KeralaCaptainBotDB']

# Original collections (unchanged from V4.1)
media_collection        = db['media']
media_backup_collection = db['media_backup']
user_conversations_col  = db['conversations']
settings_collection     = db['settings']

# Per-bot bandwidth tracking.
# Documents: { "_id": "BotUsername", "bandwidth_used": int, "is_dead": bool, ... }
# Each bot uses its own Telegram @username as the document _id.
# New bot = new username = new record = starts at 0 GB automatically.
bandwidth_collection = db['bandwidth']

# Permanent global lifetime stats. ONE document ever: { "_id": "global_stats" }
# Every bot writes to this SAME document using $inc.
# The "Lifetime Stats" admin button reads this document — so it always shows
# the combined total bandwidth and total streams across ALL bots ever deployed.
lifetime_stats_collection = db['lifetime_stats']


# ================================================================================
# ORIGINAL DATABASE FUNCTIONS (unchanged from V4.1)
# ================================================================================

async def check_duplicate(tmdb_id):
    """Checks for duplicates only in the main collection."""
    return await media_collection.find_one({"tmdb_id": tmdb_id})


async def add_media_to_db(data):
    """Inserts new media data into both the main and backup collections."""
    await media_collection.insert_one(data)
    await media_backup_collection.insert_one(data)


async def get_media_by_post_id(post_id: int):
    """Reads media data from the main collection."""
    return await media_collection.find_one({"wp_post_id": post_id})


async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str):
    """Updates links in both the main and backup collections."""
    update_query = {"$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}}
    await media_collection.update_one({"wp_post_id": post_id}, update_query)
    await media_backup_collection.update_one({"wp_post_id": post_id}, update_query)


async def delete_media_from_db(post_id: int):
    """Deletes media data from both the main and backup collections."""
    result_main = await media_collection.delete_one({"wp_post_id": post_id})
    await media_backup_collection.delete_one({"wp_post_id": post_id})
    return result_main


async def get_stats():
    """Calculates stats based only on the main collection."""
    movies_count = await media_collection.count_documents({"type": "movie"})
    series_count = await media_collection.count_documents({"type": "series"})
    return movies_count, series_count


async def get_all_media_for_library(page: int = 0, limit: int = 10):
    """Fetches the library list from the main collection."""
    cursor = media_collection.find().sort("added_at", -1).skip(page * limit).limit(limit)
    return await cursor.to_list(length=limit)


async def get_user_conversation(chat_id):
    """Gets user conversation state from DB."""
    return await user_conversations_col.find_one({"_id": chat_id})


async def update_user_conversation(chat_id, data):
    """Sets or clears user conversation state in DB."""
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})


async def get_post_id_from_msg_id(msg_id: int):
    """Helper for stream refresh — finds which post_id owns a given message_id."""
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None


async def get_protected_domain() -> str:
    """Fetches the protected domain from DB settings; falls back to Config default."""
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}. Using default.")
    return Config.PROTECTED_DOMAIN


async def set_protected_domain(new_domain: str) -> str:
    """Saves a new protected domain to the database and updates the global variable."""
    global CURRENT_PROTECTED_DOMAIN
    if not (new_domain.startswith("https://") or new_domain.startswith("http://")):
        new_domain = "https://" + new_domain
    if not new_domain.endswith('/'):
        new_domain += '/'
    await settings_collection.update_one(
        {"_id": "bot_settings"},
        {"$set": {"protected_domain": new_domain}},
        upsert=True
    )
    CURRENT_PROTECTED_DOMAIN = new_domain
    LOGGER.info(f"Protected domain updated in DB: {new_domain}")
    return new_domain


# ================================================================================
# BANDWIDTH TRACKING & AUTO-KILL (Dead Mode)
# ================================================================================

# Set during startup to this bot's Telegram @username.
# This is the MongoDB document _id for this bot's bandwidth record.
# A brand-new bot deployment (new BOT_TOKEN = new username) automatically
# gets a fresh record with bandwidth_used = 0. The old bot's record is
# left in the DB but never touched again.
BOT_USERNAME = ""

# In-memory bandwidth accumulator.
# We do NOT write to MongoDB on every single chunk — we accumulate here
# and flush to DB every BANDWIDTH_FLUSH_EVERY bytes (500 MB) to reduce
# database write pressure.
_bandwidth_in_memory = 0

# Tracks bytes accumulated since the last DB flush.
_bandwidth_since_flush = 0

# Dead Mode flag. When True, ALL /stream/ requests return 503 immediately.
# Set to True either automatically at 90 GB or manually by admin.
# Loaded from MongoDB on startup — survives restarts.
IS_DEAD = False

# Prevents the 85 GB warning from being sent more than once.
_warning_85gb_sent = False


async def load_bandwidth_state():
    """
    Loads this bot's bandwidth counter and Dead Mode state from MongoDB.
    NEW LOGIC: Uses the Render STREAM_URL as the unique DB ID instead of BOT_USERNAME.
    This ensures that deploying on a new Render URL automatically resets the counter to 0.
    """
    global _bandwidth_in_memory, IS_DEAD, _warning_85gb_sent

    if not Config.STREAM_URL:
        LOGGER.error("[BANDWIDTH] STREAM_URL not set. Cannot load bandwidth state.")
        return

    # Use STREAM_URL to find the document
    doc = await bandwidth_collection.find_one({"_id": Config.STREAM_URL})
    if doc:
        _bandwidth_in_memory = doc.get("bandwidth_used", 0)
        IS_DEAD              = doc.get("is_dead", False)
        _warning_85gb_sent   = doc.get("warning_sent", False)
        LOGGER.info(
            f"[BANDWIDTH] Loaded state for URL {Config.STREAM_URL}: "
            f"Used={humanbytes(_bandwidth_in_memory)}, Dead={IS_DEAD}"
        )
    else:
        _bandwidth_in_memory = 0
        IS_DEAD              = False
        _warning_85gb_sent   = False
        await bandwidth_collection.insert_one({
            "_id":            Config.STREAM_URL, # Using URL as the unique key
            "bot_username":   BOT_USERNAME,      # Kept for info purposes
            "bandwidth_used": 0,
            "is_dead":        False,
            "warning_sent":   False,
            "created_at":     datetime.utcnow()
        })
        LOGGER.info(f"[BANDWIDTH] New URL detected ({Config.STREAM_URL}) - counter starts at 0.")

async def flush_bandwidth_to_db():
    """
    Persists the current in-memory bandwidth counter to MongoDB using STREAM_URL.
    """
    if not Config.STREAM_URL:
        return

    await bandwidth_collection.update_one(
        {"_id": Config.STREAM_URL}, # Update using URL as the key
        {"$set": {
            "bot_username":   BOT_USERNAME,
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        IS_DEAD,
            "warning_sent":   _warning_85gb_sent,
            "last_updated":   datetime.utcnow()
        }},
        upsert=True
    )


async def add_bandwidth(bytes_sent: int):
    """
    Adds bytes_sent to the in-memory counter and checks kill thresholds.

    Called from inside the stream_handler loop so even partial streams
    (user disconnects early) are counted accurately.

    Also increments the permanent lifetime global stats counter.
    """
    global _bandwidth_in_memory, _bandwidth_since_flush, IS_DEAD, _warning_85gb_sent

    if IS_DEAD:
        return  # Already dead — stop tracking

    _bandwidth_in_memory   += bytes_sent
    _bandwidth_since_flush += bytes_sent

    # Always increment lifetime stats (permanent, shared across all bots)
    asyncio.create_task(_increment_lifetime_bandwidth_db(bytes_sent))

    # Flush to MongoDB every 500 MB to reduce write frequency
    if _bandwidth_since_flush >= Config.BANDWIDTH_FLUSH_EVERY:
        _bandwidth_since_flush = 0
        await flush_bandwidth_to_db()
        LOGGER.info(f"[BANDWIDTH] Flushed to DB. Total used: {humanbytes(_bandwidth_in_memory)}")

    # ── 85 GB Warning ────────────────────────────────────────────────────────
    if not _warning_85gb_sent and _bandwidth_in_memory >= Config.BANDWIDTH_WARNING_BYTES:
        _warning_85gb_sent = True
        await flush_bandwidth_to_db()
        LOGGER.warning(f"[BANDWIDTH] WARNING threshold reached: {humanbytes(_bandwidth_in_memory)}")
        try:
            for admin_id in Config.ADMIN_IDS:
                await main_bot.send_message(
                    admin_id,
                    f"⚠️ **BANDWIDTH WARNING!**\n\n"
                    f"**Bot:** @{BOT_USERNAME}\n"
                    f"**Used:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                    f"You are approaching the **90 GB auto-kill limit.**\n"
                    f"Please **prepare to deploy a new bot** on a new Render account soon!"
                )
        except Exception as e:
            LOGGER.error(f"Could not send bandwidth warning to admin: {e}")

    # ── 90 GB Auto-Kill ──────────────────────────────────────────────────────
    if _bandwidth_in_memory >= Config.BANDWIDTH_KILL_BYTES:
        await trigger_dead_mode(reason="auto")


async def trigger_dead_mode(reason: str = "auto"):
    """
    Puts the bot into Dead Mode permanently using STREAM_URL as the ID.
    """
    global IS_DEAD

    if IS_DEAD:
        return  # Already dead

    IS_DEAD = True
    LOGGER.critical(
        f"[DEAD MODE] URL {Config.STREAM_URL} is now DEAD. "
        f"Reason: {reason}. Bandwidth used: {humanbytes(_bandwidth_in_memory)}"
    )

    # Save immediately to DB using URL
    await bandwidth_collection.update_one(
        {"_id": Config.STREAM_URL},
        {"$set": {
            "bot_username":   BOT_USERNAME,
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        True,
            "warning_sent":   _warning_85gb_sent,
            "dead_reason":    reason,
            "dead_at":        datetime.utcnow()
        }},
        upsert=True
    )

    # Notify admins
    try:
        reason_text = (
            "automatically (**90 GB** bandwidth limit reached)"
            if reason == "auto" else "**manually** by admin"
        )
        for admin_id in Config.ADMIN_IDS:
            await main_bot.send_message(
                admin_id,
                f"🔴 **BOT IS NOW IN SLEEP (DEAD) MODE**\n\n"
                f"**Bot:** @{BOT_USERNAME}\n"
                f"**URL:** `{Config.STREAM_URL}`\n"
                f"**Killed:** {reason_text}\n"
                f"**Total bandwidth used:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                f"The bot will no longer serve any video streams.\n"
                f"Deploy a new bot on a new Render account to continue service."
            )
    except Exception as e:
        LOGGER.error(f"Could not send Dead Mode notification: {e}")


def get_bandwidth_info() -> dict:
    """Returns a snapshot of current bandwidth info. Used by admin panel and /health."""
    return {
        "used":              _bandwidth_in_memory,
        "used_human":        humanbytes(_bandwidth_in_memory),
        "is_dead":           IS_DEAD,
        "warning_sent":      _warning_85gb_sent,
        "kill_threshold":    Config.BANDWIDTH_KILL_BYTES,
        "warning_threshold": Config.BANDWIDTH_WARNING_BYTES,
        "percent":           round((_bandwidth_in_memory / Config.BANDWIDTH_KILL_BYTES) * 100, 2)
    }


# ================================================================================
# LIFETIME GLOBAL STATISTICS
# ================================================================================
# ONE MongoDB document with _id="global_stats" is used by ALL bots.
# Every bot uses $inc to add to it — never overwrites it.
# This means the "Lifetime Stats" button shows the combined total of ALL bots.

async def _increment_lifetime_bandwidth_db(bytes_sent: int):
    """
    Increments the permanent shared lifetime bandwidth counter.
    Uses $inc so concurrent writes from multiple bots are safe.
    This document is NEVER deleted.
    """
    try:
        await lifetime_stats_collection.update_one(
            {"_id": "global_stats"},
            {"$inc": {"total_bandwidth_bytes": bytes_sent}},
            upsert=True
        )
    except Exception as e:
        LOGGER.error(f"[LIFETIME STATS] Failed to increment bandwidth: {e}")


async def increment_lifetime_streams():
    """
    Increments the permanent shared lifetime stream counter.
    Called once per fresh stream request (from_bytes == 0).
    Uses $inc — safe for concurrent multi-bot writes.
    """
    try:
        await lifetime_stats_collection.update_one(
            {"_id": "global_stats"},
            {"$inc": {"total_streams": 1}},
            upsert=True
        )
    except Exception as e:
        LOGGER.error(f"[LIFETIME STATS] Failed to increment streams: {e}")


async def get_lifetime_stats() -> dict:
    """Fetches and returns the global lifetime stats document from MongoDB."""
    doc = await lifetime_stats_collection.find_one({"_id": "global_stats"})
    if doc:
        return {
            "total_bandwidth_bytes": doc.get("total_bandwidth_bytes", 0),
            "total_bandwidth_human": humanbytes(doc.get("total_bandwidth_bytes", 0)),
            "total_streams":         doc.get("total_streams", 0)
        }
    return {
        "total_bandwidth_bytes": 0,
        "total_bandwidth_human": "0 B",
        "total_streams":         0
    }


# ================================================================================
# STREAMING ENGINE — ByteStreamer CLASS (COMPLETELY UNCHANGED FROM V4.1)
# ================================================================================

multi_clients          = {}
work_loads             = {}
class_cache            = {}
processed_media_groups = {}
next_client_idx        = 0
stream_errors          = 0
last_error_reset       = time.time()


class ByteStreamer:
    """
    Original ByteStreamer from V4.1. Completely unchanged.
    Do NOT modify chunk_size or any logic inside yield_file().
    """

    def __init__(self, client: Client):
        self.client: Client  = client
        self.cached_file_ids = {}
        self.session_cache   = {}
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        """Clears in-memory file property and session caches every 20 minutes."""
        while True:
            await asyncio.sleep(1200)
            self.cached_file_ids.clear()
            self.session_cache.clear()
            LOGGER.info("Cleared ByteStreamer's cached file properties and sessions.")

    async def get_file_properties(self, message_id: int) -> FileId:
        """Gets file metadata from Telegram (or in-memory cache)."""
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError(f"No media found for message_id={message_id}")

        media   = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        """Creates or reuses a Pyrogram media session for the file's DC."""
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id         = file_id.dc_id

        # Check TTL-cached session first (5-minute TTL)
        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300:
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        # Ping the existing session to verify it's still alive
        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time())
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing session for DC {dc_id} is stale: {e}. Recreating.")
                try:
                    await media_session.stop()
                except Exception:
                    pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        # Create a new session
        LOGGER.info(f"Creating new media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            media_session = Session(
                self.client, dc_id,
                await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()
            for i in range(3):
                try:
                    exported_auth = await self.client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                    )
                    await media_session.send(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id, bytes=exported_auth.bytes
                        )
                    )
                    break
                except AuthBytesInvalid as e:
                    LOGGER.warning(f"AuthBytesInvalid attempt {i+1}: {e}")
                    if i == 2:
                        raise
                    await asyncio.sleep(1)
        else:
            media_session = Session(
                self.client, dc_id,
                await self.client.storage.auth_key(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id]          = (media_session, time.time())
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        """Builds the Pyrogram raw file location for GetFile calls."""
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            return InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
        """
        COMPLETELY UNCHANGED from V4.1.
        Yields 1 MB chunks from Telegram. chunk_size is always 1024 * 1024.
        FileReferenceExpired refresh logic is untouched.
        """
        media_session  = await self.generate_media_session(file_id)
        location       = self.get_location(file_id)
        current_offset = offset
        retry_count    = 0
        max_retries    = 3

        while True:
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(
                        location=location, offset=current_offset, limit=chunk_size
                    ),
                    timeout=30
                )
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    if len(chunk.bytes) < chunk_size:
                        break
                    current_offset += len(chunk.bytes)
                else:
                    break

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    raise
                LOGGER.warning(
                    f"FileReferenceExpired for msg {message_id}, "
                    f"retry {retry_count}/{max_retries}. Refreshing..."
                )
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        self.cached_file_ids[message_id] = new_file_id

                        post_id = await get_post_id_from_msg_id(message_id)
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                old_qualities = media_doc['message_ids']
                                quality_key   = next(
                                    (k for k, v in old_qualities.items() if v == message_id), None
                                )
                                new_qualities = old_qualities
                                if quality_key:
                                    new_qualities[quality_key] = refreshed_msg.id
                                else:
                                    new_qualities = {
                                        k: refreshed_msg.id if v == message_id else v
                                        for k, v in old_qualities.items()
                                    }
                                await update_media_links_in_db(
                                    post_id, new_qualities, media_doc['stream_link']
                                )

                        location = self.get_location(new_file_id)
                        await asyncio.sleep(2)
                        continue
                raise  # Refresh failed — give up

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value}s on GetFile. Waiting...")
                await asyncio.sleep(e.value)
                continue


# ================================================================================
# WEB ROUTES
# ================================================================================

routes = web.RouteTableDef()


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(
        text="Welcome to KeralaCaptain's Streaming Service!",
        content_type='text/html'
    )


@routes.get("/health")
async def health_handler(request):
    """Health check endpoint with bandwidth and client info."""
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60:
        stream_errors    = 0
        last_error_reset = time.time()

    bw_info    = get_bandwidth_info()
    cache_size = 0
    if multi_clients:
        sample_client = list(multi_clients.values())[0]
        if sample_client in class_cache:
            cache_size = len(class_cache[sample_client].cached_file_ids)

    return web.json_response({
        "status":                 "dead" if IS_DEAD else "ok",
        "active_clients":         len(multi_clients),
        "property_cache_size":    cache_size,
        "stream_errors_last_min": stream_errors,
        "workloads":              work_loads,
        "bandwidth_used":         bw_info["used_human"],
        "bandwidth_percent":      f"{bw_info['percent']}%",
    })


@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204)


# ── FEATURE 1: /watch Route ───────────────────────────────────────────────────
# Serves player.html via aiohttp_jinja2.
# URL: /watch?id=<wp_post_id>  (also accepts ?view_id= for compatibility)
# The video_id context variable is passed to the template. The existing JS
# in player.html already reads `view_id` / `id` directly from window.location,
# so this template variable is available as extra context if needed.

@routes.get("/watch")
async def watch_handler(request: web.Request):
    """
    Renders and serves player.html.
    Captures ALL query parameters (id, view_id, s, e, etc.) and passes them to the template.
    """
    # Convert all URL query parameters into a dictionary
    query_params = dict(request.rel_url.query)
    
    # Ensure at least an ID is present
    if 'id' not in query_params and 'view_id' not in query_params:
        return web.Response(
            status=400,
            text="400 Bad Request: Missing video ID parameter."
        )
        
    return aiohttp_jinja2.render_template(
        'player.html',
        request,
        context={'query': query_params} # Pass all parameters to HTML
    )


# ── FEATURE 2: Token API ──────────────────────────────────────────────────────
# GET /api/get_token?video_id=<id>
# Returns a fresh HMAC token valid for 60 seconds.
# The player.html JS silently calls this every 45 seconds and passes the
# returned token in every segment XHR via HLS.js xhrSetup.

@routes.get("/api/get_token")
async def get_token_handler(request: web.Request):
    video_id = request.rel_url.query.get('video_id', 'unknown')

    if IS_DEAD:
        return web.json_response(
            {"error": "Service unavailable: bandwidth limit reached."},
            status=503
        )

    token = generate_stream_token(video_id)
    LOGGER.debug(f"[TOKEN] Issued token for video_id={video_id}")

    return web.json_response(
        {"token": token, "expires_in": TOKEN_MAX_AGE_SECS},
        headers={
            # Allow the bot's own URL to request the token securely
            "Access-Control-Allow-Origin": Config.STREAM_URL.rstrip('/'),
            "Cache-Control": "no-store, no-cache, must-revalidate"
        }
    )


# ── MAIN STREAM HANDLER (V4.4 — token + throttle added) ──────────────────────
@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    """
    Main streaming route — V4.4: Telegram → Server → User pipe with security gates.

    GATE ORDER:
      1. Dead Mode check       → 503
      2. Referer protection    → 403  (unchanged from V4.3)
      3. Token verification    → 403  (NEW: HMAC, 60-second rolling token)
      4. Stream + Throttle     → 206  (NEW: burst 10 s, then 0.5 s sleep/chunk)

    All existing V4.3 logic (load balancing, bandwidth tracking, FileReferenceExpired
    refresh, partial-stream counting) is completely unchanged.
    """
    global stream_errors
    client_index     = None
    total_bytes_sent = 0

    try:
        # ── GATE 1: Dead Mode ─────────────────────────────────────────────────
        if IS_DEAD:
            return web.Response(
                status=503,
                text=(
                    "⚠️ Server bandwidth limit reached.\n"
                    "This bot is no longer serving video files.\n"
                    "Please contact the admin for the updated streaming link."
                ),
                content_type='text/plain'
            )

        # ── GATE 2: Referer Protection ────────────────────────────────────────
        # In the new architecture, the player is hosted directly on the bot's /watch route.
        # Therefore, the valid referer is the bot's OWN STREAM_URL.
        referer         = request.headers.get('Referer', '')
        allowed_referer = Config.STREAM_URL.rstrip('/')

        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(
                f"Blocked hotlink attempt. Referer='{referer}', Expected to start with='{allowed_referer}'"
            )
            return web.Response(
                status=403,
                text="403 Forbidden: Direct access is not allowed. Please use the official player."
            )

        # ── GATE 3: Token Verification (FEATURE 3) ────────────────────────────
        # Every legitimate request from the player includes a fresh token injected
        # by HLS.js xhrSetup. Download managers copying a URL get an expired token
        # within 60 seconds and all subsequent requests are rejected.
        token = request.rel_url.query.get('token', '')
        if not token:
            LOGGER.warning(
                f"[STREAM] Missing token for msg_id="
                f"{request.match_info.get('message_id', '?')}. Referer='{referer}'"
            )
            return web.Response(
                status=403,
                text="403 Forbidden: Missing stream token."
            )
        if not verify_stream_token(token):
            LOGGER.warning(
                f"[STREAM] Invalid/expired token for msg_id="
                f"{request.match_info.get('message_id', '?')}. Referer='{referer}'"
            )
            return web.Response(
                status=403,
                text="403 Forbidden: Invalid or expired stream token."
            )

        # ── Parse Request ─────────────────────────────────────────────────────
        message_id   = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", 0)

        # ── Client Load Balancing (round-robin on least-loaded) ───────────────
        min_load   = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]
        global next_client_idx
        if len(candidates) > 1:
            client_index    = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]

        faster_client = multi_clients[client_index]
        work_loads[client_index] += 1

        if faster_client not in class_cache:
            class_cache[faster_client] = ByteStreamer(faster_client)
        tg_connect = class_cache[faster_client]

        # ── Get File Properties ───────────────────────────────────────────────
        file_id   = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size

        # ── Parse Range Header ────────────────────────────────────────────────
        from_bytes = 0
        if range_header:
            from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
            from_bytes = int(from_bytes_str)

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        # chunk_size = 1 MB — DO NOT CHANGE (Pyrogram offset math depends on this)
        chunk_size     = 1024 * 1024
        offset         = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset

        # ── Build Response Headers ────────────────────────────────────────────
        cors_headers = {'Access-Control-Allow-Origin': allowed_referer}

        resp = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type":   file_id.mime_type,
                "Content-Range":  f"bytes {from_bytes}-{file_size - 1}/{file_size}",
                "Content-Length": str(file_size - from_bytes),
                "Accept-Ranges":  "bytes",
                **cors_headers
            }
        )
        await resp.prepare(request)

        # ── Lifetime Stream Counter ───────────────────────────────────────────
        # Count fresh streams only (from_bytes == 0 means a new play, not a seek)
        if from_bytes == 0:
            asyncio.create_task(increment_lifetime_streams())

        # ── FEATURE 4: Burst timer ────────────────────────────────────────────
        # Every new Range request (including seeks) starts its own burst clock.
        # This means:
        #   - Initial play:       10 s full speed → throttled
        #   - User seeks forward: 10 s full speed → throttled  (seamless recovery)
        #   - Download manager:   after burst, each chunk costs +0.5 s (≈2 MB/s cap)
        connection_start_time = time.time()

        # ── Direct Pipe: Telegram → User ─────────────────────────────────────
        # V4.1 streaming loop. No disk writes. No background tasks.
        # If the user closes the player, resp.write() throws ConnectionError or
        # CancelledError — we break immediately and stop fetching from Telegram.
        # Bandwidth is tracked PER CHUNK so even a partial watch is counted.
        is_first_chunk = True

        async for chunk in tg_connect.yield_file(file_id, offset, chunk_size, message_id):
            try:
                if is_first_chunk and first_part_cut > 0:
                    data            = chunk[first_part_cut:]
                    await resp.write(data)
                    total_bytes_sent += len(data)
                    is_first_chunk    = False
                else:
                    await resp.write(chunk)
                    total_bytes_sent += len(chunk)

                # Track bandwidth on every chunk so partial streams count too.
                # add_bandwidth() handles the 500 MB flush cadence internally.
                await add_bandwidth(len(chunk) if not (is_first_chunk and first_part_cut > 0)
                                    else len(chunk) - first_part_cut)

                # ── FEATURE 4: Throttle after burst phase ─────────────────────
                # We DO NOT touch yield_file() or chunk_size — Pyrogram's offset
                # math stays intact. We simply pause the writer after the burst.
                # Real viewers: HLS buffer fills faster than playback → no stutter.
                # Download managers: sustained rate drops to ~2 MB/s → impractical.
                elapsed = time.time() - connection_start_time
                if elapsed > Config.BURST_DURATION_SECS:
                    await asyncio.sleep(Config.THROTTLE_SLEEP_SECS)

            except (ConnectionError, asyncio.CancelledError):
                # User disconnected (closed player, changed quality, etc.)
                # Stop immediately — do NOT continue fetching from Telegram.
                LOGGER.debug(
                    f"[STREAM] Client disconnected for msg_id={message_id} "
                    f"after {humanbytes(total_bytes_sent)} sent."
                )
                return resp

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        LOGGER.error(f"[STREAM] FATAL error for msg_id={message_id}: {type(e).__name__}")
        stream_errors += 1
        return web.Response(status=410, text="Stream link expired. Please refresh the page.")

    except Exception as e:
        LOGGER.critical(
            f"[STREAM] Unhandled error for msg_id={message_id}: {e}", exc_info=True
        )
        stream_errors += 1
        return web.Response(status=500)

    finally:
        # Always decrement the workload counter for the selected client
        if client_index is not None:
            work_loads[client_index] -= 1
            LOGGER.debug(f"[STREAM] Workload decremented for client {client_index}.")


async def web_server():
    """
    Creates and configures the aiohttp web application.
    V4.4: also sets up aiohttp_jinja2 with FileSystemLoader pointing to the
    directory where player.html lives (same directory as this script by default).
    Override TEMPLATES_DIR env var if player.html is in a different path.
    """
    web_app = web.Application(client_max_size=30_000_000)

    # ── aiohttp_jinja2 setup (FEATURE 1) ─────────────────────────────────────
    # Looks for player.html (and any other templates) in TEMPLATES_DIR.
    # Default: the directory where main.py lives ('.').
    templates_dir = os.environ.get("TEMPLATES_DIR", os.path.dirname(os.path.abspath(__file__)))
    aiohttp_jinja2.setup(
        web_app,
        loader=jinja2.FileSystemLoader(templates_dir)
    )
    LOGGER.info(f"[WEB] Jinja2 templates directory: {templates_dir}")

    web_app.add_routes(routes)
    return web_app


# ================================================================================
# BOT & CLIENT INITIALIZATION
# ================================================================================

main_bot = Client(
    "KeralaCaptainBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)


class TokenParser:
    def parse_from_env(self):
        return {
            c + 2: t
            for c, (_, t) in enumerate(
                filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items()))
            )
        }


async def initialize_clients():
    """Initialises the main bot + any MULTI_TOKEN_ auxiliary clients."""
    multi_clients[0] = main_bot
    work_loads[0]    = 0

    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("No MULTI_TOKEN clients found. Running in single-client mode.")
        return

    async def start_client(client_id, token):
        try:
            client = await Client(
                name=str(client_id),
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=token,
                no_updates=True,
                in_memory=True
            ).start()
            work_loads[client_id] = 0
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start Client {client_id}: {e}")
            return None

    results = await asyncio.gather(
        *[start_client(i, token) for i, token in all_tokens.items()]
    )
    multi_clients.update({cid: client for cid, client in results if client is not None})

    if len(multi_clients) > 1:
        LOGGER.info(
            f"Multi-Client mode ON: {len(multi_clients)} clients initialised."
        )


async def forward_file_safely(message_to_forward: Message):
    """
    Resends a Telegram file via the main bot to refresh its file_reference.
    Called by yield_file() when FileReferenceExpired is raised.
    Completely unchanged from V4.1.
    """
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("forward_file_safely: message has no media.")
            return None

        LOGGER.info(
            f"Sending cached media for msg {message_to_forward.id} via main bot..."
        )
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=media.file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
    except Exception as e:
        LOGGER.error(f"forward_file_safely failed: {e}")
        return None


# ================================================================================
# ADMIN BOT HANDLERS
# ================================================================================

admin_only = filters.user(Config.ADMIN_IDS)


def _get_main_menu_markup() -> InlineKeyboardMarkup:
    """Returns the main admin panel inline keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics",       callback_data="admin_stats")],
        [InlineKeyboardButton("📈 Lifetime Stats",   callback_data="admin_lifetime_stats")],
        [InlineKeyboardButton("⚙️ Settings",         callback_data="admin_settings")],
        [InlineKeyboardButton("🔄 Restart Bot",      callback_data="admin_restart")],
        [InlineKeyboardButton("🛑 Kill Bot (Sleep)", callback_data="admin_kill_bot")],
    ])


@main_bot.on_message(filters.command("start") & filters.private & admin_only)
async def start_command(client, message: Message):
    await message.reply_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )
    await update_user_conversation(message.chat.id, None)


@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    """Shows live bot statistics: bandwidth, system info, streaming workloads."""
    await cb.answer("Fetching stats...")

    uptime = get_readable_time(int(time.time() - start_time))

    try:
        cpu_usage  = psutil.cpu_percent()
        ram_usage  = psutil.virtual_memory().percent
        ram_total  = humanbytes(psutil.virtual_memory().total)
        disk_usage = psutil.disk_usage('/').percent
    except Exception:
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_total = "N/A"

    bw_info      = get_bandwidth_info()
    dead_status  = "🔴 DEAD (Sleep Mode)" if bw_info["is_dead"] else "🟢 Active"
    workload_str = "\n".join(
        [f"  - Client {cid}: {load} active streams" for cid, load in work_loads.items()]
    )

    text = (
        f"**📊 Bot Statistics**\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}  |  **Status:** {dead_status}\n"
        f"**Uptime:** `{uptime}`\n\n"
        f"**🌐 Bandwidth (This Bot):**\n"
        f"  - Used: `{bw_info['used_human']}` / 90 GB\n"
        f"  - Progress: `{bw_info['percent']}%`\n"
        f"  - 85 GB Warning Sent: `{bw_info['warning_sent']}`\n\n"
        f"**🖥️ System:**\n"
        f"  - CPU: `{cpu_usage}%`\n"
        f"  - RAM: `{ram_usage}%` (Total: `{ram_total}`)\n"
        f"  - Disk: `{disk_usage}%`\n\n"
        f"**📡 Streaming:**\n"
        f"  - Active Clients: `{len(multi_clients)}`\n"
        f"  - Stream Errors (last min): `{stream_errors}`\n"
        f"  - Workloads:\n{workload_str}"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_lifetime_stats$") & admin_only)
async def lifetime_stats_callback(client, cb: CallbackQuery):
    """
    Shows the PERMANENT global lifetime stats shared across ALL bots.

    This reads the single {"_id": "global_stats"} document from MongoDB.
    Every bot that has ever run this code has been writing $inc to this
    same document, so the numbers here represent the grand total across
    all 10 (or however many) bots you have ever deployed.
    """
    await cb.answer("Fetching lifetime stats...")

    stats = await get_lifetime_stats()

    text = (
        f"**📈 Lifetime Global Statistics**\n\n"
        f"These figures represent the combined total across "
        f"**ALL bots you have ever deployed** on this system.\n"
        f"This data is stored in MongoDB and is **permanent — never deleted.**\n\n"
        f"**💾 Total Bandwidth Served to Users:**\n"
        f"  `{stats['total_bandwidth_human']}`\n\n"
        f"**▶️ Total Video Streams Started:**\n"
        f"  `{stats['total_streams']:,}` streams"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_settings$") & admin_only)
async def settings_callback(client, cb: CallbackQuery):
    """Shows current settings and allows changing the protected domain."""
    await cb.answer()
    current_domain = await get_protected_domain()
    bw_info        = get_bandwidth_info()

    text = (
        f"**⚙️ Settings**\n\n"
        f"**Protected Domain:**\n"
        f"Streams are only allowed when the Referer header matches this domain.\n\n"
        f"Current: `{current_domain}`\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}\n"
        f"**Bandwidth Used:** `{bw_info['used_human']}`"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set New Domain", callback_data="admin_set_domain")],
            [InlineKeyboardButton("⬅️ Back",           callback_data="admin_main_menu")]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_set_domain$") & admin_only)
async def set_domain_callback(client, cb: CallbackQuery):
    """Prompts the admin to type the new protected domain."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set New Domain**\n\n"
        "Send the new protected domain as a plain text message.\n\n"
        "Example: `https://keralacaptain.in` or just `keralacaptain.in`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_kill_bot$") & admin_only)
async def kill_bot_callback(client, cb: CallbackQuery):
    """Shows the Kill Bot confirmation dialog."""
    await cb.answer()

    if IS_DEAD:
        await cb.message.edit_text(
            "🔴 **This bot is already in Sleep (Dead) Mode.**\n\n"
            "It is not serving any video streams.\n"
            "Deploy a new bot to resume service.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
            )
        )
        return

    bw_info = get_bandwidth_info()
    await cb.message.edit_text(
        f"**⚠️ Are you sure you want to kill this bot?**\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}\n"
        f"**Bandwidth Used:** `{bw_info['used_human']}`\n\n"
        f"The bot will **permanently stop serving all video files.**\n"
        f"This state is saved to MongoDB and **survives restarts.**\n\n"
        f"You will need to deploy a new bot to continue service.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Kill It",  callback_data="admin_kill_bot_confirm"),
                InlineKeyboardButton("❌ No, Cancel",    callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_kill_bot_confirm$") & admin_only)
async def kill_bot_confirm_callback(client, cb: CallbackQuery):
    """Admin confirmed kill — triggers Dead Mode manually."""
    await cb.answer("Killing bot...")

    await cb.message.edit_text(
        "🔴 **Bot is now in Sleep (Dead) Mode.**\n\n"
        "All video streams have been blocked immediately.\n"
        "This state is saved to the database and will persist on restart.\n\n"
        "Deploy a new bot on a new Render account to continue service."
    )

    # Trigger Dead Mode with reason="manual"
    await trigger_dead_mode(reason="manual")


@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    """Shows restart confirmation."""
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Are you sure you want to restart the bot?**\n\n"
        "The bandwidth counter will be safely flushed to MongoDB before restart.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Restart", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No, Go Back",  callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    """Admin confirmed restart — flushes bandwidth to DB then restarts process."""
    await cb.answer("Restarting...")
    await cb.message.edit_text("✅ **Restarting...**\n\nBot will be back online shortly.")

    try:
        LOGGER.info("RESTART triggered by admin.")
        # Flush bandwidth counter to DB so no data is lost across the restart
        await flush_bandwidth_to_db()
        LOGGER.info("Bandwidth flushed to DB before restart.")
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
    except Exception as e:
        LOGGER.error(f"Error during pre-restart cleanup: {e}")

    # Replace the current process with a fresh instance of itself
    os.execl(sys.executable, sys.executable, *sys.argv)


@main_bot.on_callback_query(
    filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only
)
async def main_menu_callback(client, cb: CallbackQuery):
    """Returns to the main admin menu and clears any active conversation state."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )


@main_bot.on_message(filters.private & filters.text & admin_only)
async def text_message_handler(client, message: Message):
    """Handles free-text input from admin during active conversation flows."""
    chat_id = message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return

    stage = conv.get("stage")

    if stage == "awaiting_domain":
        new_domain = message.text.strip()
        if "." not in new_domain or " " in new_domain:
            return await message.reply_text(
                "❌ Invalid format. Please send a valid domain like `keralacaptain.in`."
            )
        try:
            status_msg   = await message.reply_text("⏳ Saving...")
            saved_domain = await set_protected_domain(new_domain)
            await status_msg.edit_text(
                f"✅ **Success!**\n\nProtected domain updated to:\n`{saved_domain}`",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]]
                )
            )
            await update_user_conversation(chat_id, None)
        except Exception as e:
            await message.reply_text(f"❌ **Error!**\nCould not save domain: `{e}`")


# ================================================================================
# APPLICATION LIFECYCLE
# ================================================================================

async def ping_server():
    """Keeps the Render/Heroku dyno alive by self-pinging at a regular interval."""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(
                timeout=ClientTimeout(total=10)
            ) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"[PING] Self-ping status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"[PING] Self-ping failed: {e}")


if __name__ == "__main__":

    async def main_startup_shutdown_logic():
        """Full startup sequence. Runs until SIGINT/SIGTERM is received."""
        global CURRENT_PROTECTED_DOMAIN, BOT_USERNAME

        LOGGER.info("========== KeralaCaptain Bot V4.4 Starting ==========")

        # ── Step 1: Load protected domain from DB ─────────────────────────────
        CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
        LOGGER.info(f"Protected domain: {CURRENT_PROTECTED_DOMAIN}")

        # ── Step 2: Ensure MongoDB indexes ────────────────────────────────────
        await media_collection.create_index("tmdb_id", unique=True)
        await media_collection.create_index("wp_post_id", unique=True)
        LOGGER.info("MongoDB indexes ensured.")

        # ── Step 3: Start main bot and get its username ───────────────────────
        try:
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started.")
        except FloodWait as e:
            LOGGER.warning(f"FloodWait on startup: {e.value}s. Waiting...")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started after wait.")
        except Exception as e:
            LOGGER.critical(f"Failed to start main bot: {e}", exc_info=True)
            raise

        # ── Step 4: Load bandwidth state from MongoDB ─────────────────────────
        await load_bandwidth_state()
        LOGGER.info(
            f"Bandwidth state: Used={humanbytes(_bandwidth_in_memory)}, "
            f"Dead={IS_DEAD}, WarningSent={_warning_85gb_sent}"
        )

        # ── Step 5: Initialize multi-client streaming ─────────────────────────
        await initialize_clients()

        # ── Step 6: Start keep-alive ping (Render/Heroku) ────────────────────
        if Config.ON_HEROKU:
            asyncio.create_task(ping_server())

        # ── Step 7: Start the aiohttp web server ──────────────────────────────
        web_app = await web_server()
        runner  = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")

        # ── Step 8: Send startup notification to admin ────────────────────────
        try:
            bw_info     = get_bandwidth_info()
            dead_notice = (
                "\n\n🔴 **WARNING: This bot is STILL in Dead Mode from the previous run!**"
                if IS_DEAD else ""
            )
            await main_bot.send_message(
                Config.ADMIN_IDS[0],
                f"**✅ Bot @{BOT_USERNAME} is online! (V4.4)**\n\n"
                f"**Bandwidth Used:** `{bw_info['used_human']}` / 90 GB\n"
                f"**Status:** {'🔴 DEAD' if IS_DEAD else '🟢 Active'}"
                f"{dead_notice}"
            )
        except Exception as e:
            LOGGER.warning(f"Could not send startup notification: {e}")

        LOGGER.info("========== Bot is fully operational. ==========")

        # Wait forever until SIGINT/SIGTERM
        await asyncio.Event().wait()

    # ── Event loop and signal handling ────────────────────────────────────────
    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received signal {sig.name}. Shutting down gracefully...")

        # Always flush bandwidth to DB on shutdown so no data is lost
        await flush_bandwidth_to_db()
        LOGGER.info("Bandwidth flushed to DB on shutdown.")

        if main_bot and main_bot.is_connected:
            LOGGER.info("Stopping main bot...")
            await main_bot.stop()

        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} outstanding tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)

        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s))
        )

    try:
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"Critical error forced shutdown: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped.")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete. Goodbye!")
