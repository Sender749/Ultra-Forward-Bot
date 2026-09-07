#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

"""
plugins/gen_session.py — /gen_session

Generates the Pyrogram session string that user_session.py needs (the
SESSION_STRING env var used by /member_forward), entirely inside the bot.

Rewritten to use a plain session-dict + inline-button flow (same pattern as
the captionbot repo's /gen_session, and as plugins/member_forward.py here)
instead of client.ask()/client.listen() — this makes the whole flow behave
identically to a normal command handler, with nothing depending on this
particular pyrofork build's conversation-helper behaving a certain way.

This is deliberately separate from the existing /settings → "add user bot"
flow (plugins/test.py's add_login): that flow saves the session into this
bot's per-user clone-bot database and immediately uses it for the regular
forwarder. This command does neither — it just hands you the string.
Nothing is stored.

Flow (bot-owner only, private chat)
────────────────────────────────────────────────────────────────────────────
  /gen_session
    → send the phone number of the account to log in as
    → send the login code Telegram sends to that account
    → (only if 2FA is on) send that account's 2-step-verification password
    → the bot replies with the session string + setup instructions

Security
────────────────────────────────────────────────────────────────────────────
- A temporary, in-memory Pyrogram client is used (no .session file is ever
  written to disk) and is disconnected immediately once done.
- The phone/code/password messages are best-effort deleted right after
  being read.
- The resulting string is exactly as sensitive as that account's password.
"""

import asyncio
import logging
import re
import time

from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait,
    PhoneNumberInvalid,
    PhoneNumberBanned,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PasswordHashInvalid,
)
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config

logger = logging.getLogger(__name__)

_ADMIN_FILTER = filters.user(Config.BOT_OWNER_ID)
_PHONE_RE = re.compile(r"^\+?[1-9]\d{7,14}$")
_SESSION_TTL = 600  # 10 minutes — logins involve waiting on an SMS/app code

# uid -> {"step","client","phone","phone_code_hash","expires"}
GS_SESSIONS = {}


async def _cleanup(uid: int, disconnect: bool = True):
    s = GS_SESSIONS.pop(uid, None)
    if s and disconnect:
        tmp = s.get("client")
        if tmp is not None:
            try:
                await tmp.disconnect()
            except Exception:
                pass


async def _expire_after(uid: int, marker: dict):
    await asyncio.sleep(_SESSION_TTL)
    if GS_SESSIONS.get(uid) is marker:
        await _cleanup(uid)


# ── /gen_session ───────────────────────────────────────────────────────────
@Client.on_message(filters.private & _ADMIN_FILTER & filters.command("gen_session"))
async def gen_session_cmd(client: Client, message):
    uid = message.from_user.id
    await _cleanup(uid)  # drop any stale in-progress attempt first

    tmp = Client(
        name=f"gensession_{uid}_{int(time.time())}",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        in_memory=True,
    )
    try:
        await tmp.connect()
    except Exception as e:
        return await message.reply_text(f"❌ Couldn't start a Telegram connection:\n<code>{e}</code>",
                                         parse_mode=ParseMode.HTML)

    s = {"step": "phone", "client": tmp, "expires": time.time() + _SESSION_TTL}
    GS_SESSIONS[uid] = s
    asyncio.create_task(_expire_after(uid, s))

    await message.reply_text(
        "🔑 <b>Generate Session String</b>\n\n"
        "Send the phone number of the account to log in as, in "
        "international format:\n<code>+919876543210</code>\n\n"
        "⚠️ This can be a <b>different</b> account than this bot — it's "
        "whichever account you want <code>/member_forward</code> to act as.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="gs_cancel")]]),
    )


@Client.on_callback_query(filters.regex("^gs_cancel$"))
async def gs_cancel_cb(client: Client, query):
    uid = query.from_user.id
    if uid in GS_SESSIONS:
        await _cleanup(uid)
        await query.message.edit_text("🛑 Cancelled. No session was generated.")
    else:
        await query.message.edit_text("❌ Nothing to cancel.")
    await query.answer()


# ── text dispatch (called from plugins/member_forward.py's shared dispatcher) ──
async def handle_gen_session_text(client: Client, message) -> bool:
    uid = message.from_user.id
    s = GS_SESSIONS.get(uid)
    if not s:
        return False

    if s.get("expires") and s["expires"] < time.time():
        await _cleanup(uid)
        await message.reply_text("⏰ Session expired.\nStart again using /gen_session")
        return True

    step = s.get("step")
    if step == "phone":
        await _handle_phone(message, uid, s)
        return True
    if step == "code":
        await _handle_code(message, uid, s)
        return True
    if step == "password":
        await _handle_password(message, uid, s)
        return True
    return False


async def _safe_delete(message):
    try:
        await message.delete()
    except Exception:
        pass


async def _handle_phone(message, uid: int, s: dict):
    phone = (message.text or "").strip().replace(" ", "")
    if not _PHONE_RE.match(phone):
        await message.reply_text(
            "❌ That doesn't look like a valid phone number. Send it in "
            "international format, e.g. <code>+919876543210</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    tmp = s["client"]
    try:
        sent = await tmp.send_code(phone)
    except FloodWait as e:
        await message.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                  parse_mode=ParseMode.HTML)
        return
    except (PhoneNumberInvalid, PhoneNumberBanned) as e:
        await _cleanup(uid)
        await message.reply_text(f"❌ {type(e).__name__}: that number can't be used.\nStart again with /gen_session")
        return
    except Exception as e:
        await _cleanup(uid)
        await message.reply_text(f"❌ Failed to send login code:\n<code>{e}</code>\n\nStart again with /gen_session",
                                  parse_mode=ParseMode.HTML)
        return

    await _safe_delete(message)

    s["phone"] = phone
    s["phone_code_hash"] = sent.phone_code_hash
    s["step"] = "code"
    s["expires"] = time.time() + _SESSION_TTL
    await message.reply_text(
        "📩 Code sent. Enter the login code Telegram just sent to that "
        "account (as digits, no spaces or dashes).",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="gs_cancel")]]),
    )


async def _handle_code(message, uid: int, s: dict):
    code = (message.text or "").strip().replace(" ", "").replace("-", "")
    tmp = s["client"]
    await _safe_delete(message)

    try:
        await tmp.sign_in(s["phone"], s["phone_code_hash"], code)
    except SessionPasswordNeeded:
        s["step"] = "password"
        s["expires"] = time.time() + _SESSION_TTL
        await message.reply_text(
            "🔒 This account has two-step verification enabled.\nSend its 2FA password.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="gs_cancel")]]),
        )
        return
    except PhoneCodeInvalid:
        await message.reply_text("❌ Wrong code. Try again.")
        return
    except PhoneCodeExpired:
        await _cleanup(uid)
        await message.reply_text("❌ Code expired.\nStart again with /gen_session")
        return
    except FloodWait as e:
        await message.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                  parse_mode=ParseMode.HTML)
        return
    except Exception as e:
        await _cleanup(uid)
        await message.reply_text(f"❌ Sign-in failed:\n<code>{e}</code>\n\nStart again with /gen_session",
                                  parse_mode=ParseMode.HTML)
        return

    await _finish(message, uid, s)


async def _handle_password(message, uid: int, s: dict):
    password = (message.text or "").strip()
    tmp = s["client"]
    await _safe_delete(message)

    try:
        await tmp.check_password(password)
    except PasswordHashInvalid:
        await message.reply_text("❌ Wrong password. Try again.")
        return
    except FloodWait as e:
        await message.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                  parse_mode=ParseMode.HTML)
        return
    except Exception as e:
        await _cleanup(uid)
        await message.reply_text(f"❌ Sign-in failed:\n<code>{e}</code>\n\nStart again with /gen_session",
                                  parse_mode=ParseMode.HTML)
        return

    await _finish(message, uid, s)


async def _finish(message, uid: int, s: dict):
    tmp = s["client"]
    try:
        session_string = await tmp.export_session_string()
    except Exception as e:
        await _cleanup(uid)
        await message.reply_text(f"❌ Login succeeded but exporting the session failed:\n<code>{e}</code>",
                                  parse_mode=ParseMode.HTML)
        return

    await _cleanup(uid, disconnect=True)

    await message.reply_text(
        "✅ <b>Logged in — session string generated:</b>\n\n"
        f"<code>{session_string}</code>\n\n"
        "⚠️ <b>Treat this like a password.</b> Anyone with it has full "
        "access to that Telegram account.\n\n"
        "<b>Next steps:</b>\n"
        "1. Copy the string above.\n"
        "2. Set it as the <code>SESSION_STRING</code> environment variable.\n"
        "3. Restart the bot.\n"
        "4. Delete this message once you've copied it.",
        parse_mode=ParseMode.HTML,
    )
    logger.info(f"[GEN_SESSION] owner={uid} generated a new session string")


# ── diagnostic: fires ONLY for non-owners, self-explains the silent-ignore ──
@Client.on_message(filters.private & filters.command("gen_session") & ~_ADMIN_FILTER)
async def gen_session_not_owner(client: Client, message):
    await message.reply_text(
        "❌ This command is restricted to the bot owner.\n\n"
        f"Your Telegram ID is <code>{message.from_user.id}</code>. If this "
        f"should be allowed, add it to the <code>BOT_OWNER_ID</code> "
        f"environment variable and restart the bot.",
        parse_mode=ParseMode.HTML,
    )
