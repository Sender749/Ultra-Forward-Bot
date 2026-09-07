#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

"""
plugins/gen_session.py — /gen_session

Generates the Pyrogram session string that user_session.py needs (the
SESSION_STRING env var used by /member_forward), entirely inside the bot.

This is deliberately separate from the existing /settings → "add user bot"
flow (plugins/test.py's add_login): that flow saves the session into this
bot's per-user clone-bot database and immediately uses it for the regular
forwarder. This command does neither — it just hands you the string, so
you can review it and set it as SESSION_STRING yourself. Nothing is stored.

Flow (bot-owner only, private chat)
────────────────────────────────────────────────────────────────────────────
  /gen_session
    → send the phone number of the account to log in as (international
      format, e.g. +919876543210 — can be any account, not necessarily
      this bot's owner's)
    → send the login code Telegram sends to that account
    → (only if 2FA is on) send that account's 2-step-verification password
    → the bot replies with the session string + setup instructions

Security
────────────────────────────────────────────────────────────────────────────
- A temporary, in-memory Pyrogram client is used (no .session file is ever
  written to disk) and is disconnected immediately once done.
- The phone/code/password messages are best-effort deleted right after
  being read.
- The resulting string is exactly as sensitive as that account's password —
  copy it, set it, then delete the message.
"""

import asyncio
import logging
import re

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

from config import Config

logger = logging.getLogger(__name__)

_ADMIN_FILTER = filters.user(Config.BOT_OWNER_ID)
_PHONE_RE = re.compile(r"^\+?[1-9]\d{7,14}$")


async def _safe_delete(message):
    try:
        await message.delete()
    except Exception:
        pass


@Client.on_message(filters.private & _ADMIN_FILTER & filters.command("gen_session"))
async def gen_session_cmd(client: Client, message):
    chat_id = message.chat.id

    try:
        phone_msg = await client.ask(
            chat_id,
            "🔑 <b>Generate Session String</b>\n\n"
            "Send the phone number of the account to log in as, in "
            "international format:\n<code>+919876543210</code>\n\n"
            "⚠️ This can be a <b>different</b> account than this bot — "
            "it's whichever account you want /member_forward to act as.\n\n"
            "/cancel to stop.",
            filters=filters.text,
            timeout=300,
        )
    except asyncio.TimeoutError:
        return await message.reply_text("⏰ Timed out.")

    phone = phone_msg.text.strip().replace(" ", "")
    if phone == "/cancel":
        return await phone_msg.reply_text("🛑 Cancelled.")
    if not _PHONE_RE.match(phone):
        return await phone_msg.reply_text(
            "❌ That doesn't look like a valid phone number. Start again with /gen_session."
        )

    tmp = Client(
        name=f"gensession_{message.from_user.id}",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        in_memory=True,   # never writes a .session file to disk
    )
    try:
        await tmp.connect()
    except Exception as e:
        return await phone_msg.reply_text(f"❌ Couldn't start a Telegram connection:\n<code>{e}</code>",
                                           parse_mode=ParseMode.HTML)

    try:
        sent = await tmp.send_code(phone)
    except FloodWait as e:
        await tmp.disconnect()
        return await phone_msg.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                           parse_mode=ParseMode.HTML)
    except (PhoneNumberInvalid, PhoneNumberBanned) as e:
        await tmp.disconnect()
        return await phone_msg.reply_text(f"❌ {type(e).__name__}: that number can't be used.")
    except Exception as e:
        await tmp.disconnect()
        return await phone_msg.reply_text(f"❌ Failed to send login code:\n<code>{e}</code>",
                                           parse_mode=ParseMode.HTML)

    await _safe_delete(phone_msg)  # phone number leaves the chat as soon as it's read

    try:
        code_msg = await client.ask(
            chat_id,
            "📩 Code sent. Enter the login code Telegram just sent to that "
            "account, as digits (no spaces/dashes).\n\n/cancel to stop.",
            filters=filters.text,
            timeout=600,
        )
    except asyncio.TimeoutError:
        await tmp.disconnect()
        return await message.reply_text("⏰ Timed out.")

    code = code_msg.text.strip().replace(" ", "").replace("-", "")
    if code == "/cancel":
        await tmp.disconnect()
        return await code_msg.reply_text("🛑 Cancelled.")
    await _safe_delete(code_msg)  # code leaves the chat as soon as it's read

    needs_password = False
    try:
        await tmp.sign_in(phone, sent.phone_code_hash, code)
    except SessionPasswordNeeded:
        needs_password = True
    except PhoneCodeInvalid:
        await tmp.disconnect()
        return await message.reply_text("❌ Wrong code. Start again with /gen_session.")
    except PhoneCodeExpired:
        await tmp.disconnect()
        return await message.reply_text("❌ Code expired. Start again with /gen_session.")
    except FloodWait as e:
        await tmp.disconnect()
        return await message.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                         parse_mode=ParseMode.HTML)
    except Exception as e:
        await tmp.disconnect()
        return await message.reply_text(f"❌ Sign-in failed:\n<code>{e}</code>", parse_mode=ParseMode.HTML)

    if needs_password:
        try:
            pwd_msg = await client.ask(
                chat_id,
                "🔒 This account has two-step verification enabled.\n"
                "Send its 2FA password.\n\n/cancel to stop.",
                filters=filters.text,
                timeout=300,
            )
        except asyncio.TimeoutError:
            await tmp.disconnect()
            return await message.reply_text("⏰ Timed out.")

        password = pwd_msg.text.strip()
        if password == "/cancel":
            await tmp.disconnect()
            return await pwd_msg.reply_text("🛑 Cancelled.")
        await _safe_delete(pwd_msg)  # password leaves the chat as soon as it's read

        try:
            await tmp.check_password(password)
        except PasswordHashInvalid:
            await tmp.disconnect()
            return await message.reply_text("❌ Wrong password. Start again with /gen_session.")
        except FloodWait as e:
            await tmp.disconnect()
            return await message.reply_text(f"⏳ Rate-limited. Wait <b>{int(e.value)}s</b> and try again.",
                                             parse_mode=ParseMode.HTML)
        except Exception as e:
            await tmp.disconnect()
            return await message.reply_text(f"❌ Sign-in failed:\n<code>{e}</code>", parse_mode=ParseMode.HTML)

    try:
        session_string = await tmp.export_session_string()
    except Exception as e:
        await tmp.disconnect()
        return await message.reply_text(f"❌ Login succeeded but exporting the session failed:\n<code>{e}</code>",
                                         parse_mode=ParseMode.HTML)

    await tmp.disconnect()

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
    logger.info(f"[GEN_SESSION] owner={message.from_user.id} generated a new session string")
