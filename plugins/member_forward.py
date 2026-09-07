#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

"""
plugins/member_forward.py — /member_forward

Forwards files out of a channel the BOT ITSELF isn't in, but a personal
Telegram account (logged in via SESSION_STRING, see user_session.py) is —
into any channel you've already registered via /settings → channels.

Built for speed
────────────────────────────────────────────────────────────────────────────
The generic forwarder in plugins/regix.py copies one message per API call
and (for userbot clients) sleeps a flat 10s after every single file — for
1000 files that's ~1000 API round trips plus ~2.8 hours of pure sleep.

This command instead:
  • SCANS in batches — one get_messages() call fetches up to 100 message
    ids at once, instead of one call per id.
  • FORWARDS in batches — Pyrofork's forward_messages(..., drop_author=True)
    moves up to 100 messages in ONE Telegram API call, with no visible
    "Forwarded from" tag (indistinguishable from a copy). No per-file sleep
    at all in the common case.
  • Falls back to one-by-one only for whatever handful of messages fail
    inside a batch (deleted mid-scan, etc.), and to download+re-upload only
    if the userbot genuinely can't write to the destination.

Flow (bot-owner only, private chat)
────────────────────────────────────────────────────────────────────────────
  /member_forward
    → send the SOURCE channel: @username / t.me link / invite link / -100 ID
      (must be a chat the userbot account is already a member of)
    → pick the DESTINATION from your /settings → channels list
    → send a range: 0 (all) / a message id (start after it) / "start - end"
    → background scan + a small worker pool forwards everything

Isolated from every other feature: its own Mongo collection
(member_forward_queue, see database.py), its own worker pool, its own
userbot connection (user_session.py). Nothing here touches regix.py's
existing forwarder, caption settings, or the main Bot's speed.
"""

import asyncio
import logging
import math
import os
import time
import uuid

from pyrogram import Client, enums, filters
from pyrogram.enums import ParseMode, ListenerTypes
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from database import db
from user_session import user_client, USER_SESSION_ENABLED

logger = logging.getLogger(__name__)

_ADMIN_FILTER = filters.user(Config.BOT_OWNER_ID)

SCAN_CHUNK          = 100   # message ids fetched per get_messages() call
FORWARD_BATCH       = 100   # message ids forwarded per forward_messages() call
CHANNELS_PER_PAGE   = 10
MEMBER_FORWARD_WORKERS = 3
MEMBER_FORWARD_DELAY   = 0.4   # cooldown per worker between batch sends (not per-file!)
MAX_EMPTY_CHUNKS       = 5     # 5 × SCAN_CHUNK = 500 consecutive missing ids ⇒ assume end of channel
PROGRESS_EVERY_FILES   = 200   # throttle progress edits (Telegram rate-limits message edits too)

# uid -> {"session_id","chat_id","msg_id","source_title","destination_title",
#         "total_files","done_files","scan_done"}  — progress/cancel state only.
# Input collection (source/destination/range) is fully linear via client.ask()
# / client.listen(), so no session dict is needed for that part at all.
MF_ACTIVE_SESSIONS = {}
MF_CANCELLED_SESSIONS = set()


def start_member_forward_workers(bot_client: Client):
    """Called once from bot.py's Bot.start(), after start_user_session()."""
    if not USER_SESSION_ENABLED:
        logger.info("[MF] SESSION_STRING not set — member-forward workers not started")
        return

    async def _guarded(i):
        while True:
            try:
                await _member_forward_worker(bot_client)
            except Exception as e:
                logger.warning(f"[MF_WORKER_{i}] crashed, restarting in 3s: {e}")
            else:
                logger.warning(f"[MF_WORKER_{i}] exited unexpectedly, restarting in 3s")
            await asyncio.sleep(3)

    for i in range(MEMBER_FORWARD_WORKERS):
        asyncio.create_task(_guarded(i), name=f"mf_worker_{i}")
    logger.info(f"[MF] {MEMBER_FORWARD_WORKERS} member-forward workers started")


# ── /member_forward — fully linear via client.ask()/client.listen() ─────────
@Client.on_message(filters.private & _ADMIN_FILTER & filters.command("member_forward"))
async def member_forward_cmd(client: Client, message):
    if not USER_SESSION_ENABLED:
        return await message.reply_text(
            "❌ <b>Member-channel forwarding isn't set up.</b>\n\n"
            "Generate a session string with /gen_session (or set it up "
            "yourself), set it as the <code>SESSION_STRING</code> "
            "environment variable, then restart the bot.",
            parse_mode=ParseMode.HTML,
        )

    uid = message.from_user.id
    chat_id = message.chat.id

    if uid in MF_ACTIVE_SESSIONS:
        return await message.reply_text(
            "❌ You already have a member-forward job running. "
            "Cancel it first (use the ❌ on its progress message)."
        )

    # ── 1. source channel ──
    try:
        src_msg = await client.ask(
            chat_id,
            "📡 <b>Member-Channel Forward</b>\n\n"
            "Send the <b>source channel</b> — one your personal account "
            "(the userbot) is a member/admin of, but this bot is <u>not</u> "
            "added to.\n\n"
            "You can send a <code>@username</code>, a <code>t.me/...</code> "
            "link or invite link, or a numeric channel ID.\n\n/cancel to stop.",
            filters=filters.text,
            timeout=300,
        )
    except asyncio.TimeoutError:
        return await message.reply_text("⏰ Timed out. Start again with /member_forward")

    raw_src = src_msg.text.strip()
    if raw_src == "/cancel":
        return await src_msg.reply_text("🛑 Cancelled.")

    try:
        chat = await user_client.get_chat(raw_src)
    except FloodWait as e:
        return await src_msg.reply_text(
            f"⏳ Telegram is rate-limiting the userbot account. Wait "
            f"<b>{int(e.value)}s</b> and try /member_forward again.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        return await src_msg.reply_text(
            f"❌ Couldn't access that channel with the userbot account.\n"
            f"<code>{e}</code>\n\n"
            "Make sure the personal account behind SESSION_STRING is a "
            "member there, then try /member_forward again.",
            parse_mode=ParseMode.HTML,
        )

    if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
        return await src_msg.reply_text("❌ That's not a channel/supergroup.")

    source = chat.id
    source_title = chat.title or str(source)

    # ── 2. destination channel (paginated picker from your /settings list) ──
    channels = await db.get_user_channels(uid)
    channels = [c for c in channels if c["chat_id"] != source]
    if not channels:
        return await src_msg.reply_text(
            "❌ No destination channels registered.\n\n"
            "Add one first: /settings → channels → ✚ Add Channel."
        )

    picked = await _pick_destination(client, uid, chat_id, channels, source_title)
    if picked is None:
        return  # already told the admin (cancelled/timeout)
    destination, destination_title = picked

    # ── 3. range ──
    try:
        range_msg = await client.ask(
            chat_id,
            "⏭ <b>Forwarding range</b>\n\n"
            "• <code>0</code> — forward ALL files\n"
            "• <code>&lt;id&gt;</code> — start AFTER this message id\n"
            "• <code>start - end</code> — forward BETWEEN two ids (inclusive)\n\n"
            "/cancel to stop.",
            filters=filters.text,
            timeout=300,
        )
    except asyncio.TimeoutError:
        return await message.reply_text("⏰ Timed out. Start again with /member_forward")

    if range_msg.text.strip() == "/cancel":
        return await range_msg.reply_text("🛑 Cancelled.")

    try:
        start_id, end_id = _parse_range(range_msg.text)
    except ValueError as e:
        return await range_msg.reply_text(f"❌ {e}\n\nStart again with /member_forward")

    # ── 4. go ──
    session_id = str(uuid.uuid4())
    progress = await client.send_message(chat_id, "🔄 Scanning source channel…")
    MF_ACTIVE_SESSIONS[uid] = {
        "session_id": session_id,
        "chat_id": chat_id,
        "msg_id": progress.id,
        "source_title": source_title,
        "destination_title": destination_title,
        "total_files": 0,
        "done_files": 0,
        "scan_done": False,
    }
    await _update_progress_markup(client, uid)
    asyncio.create_task(
        _scan_and_enqueue(client, uid, session_id, source, destination, start_id, end_id),
        name=f"mf_scan_{uid}_{session_id[:8]}",
    )


def _parse_range(raw: str):
    raw = raw.strip()
    if raw == "0":
        return 1, None
    if "-" in raw:
        parts = raw.split("-")
        if len(parts) != 2 or not parts[0].strip().isdigit() or not parts[1].strip().isdigit():
            raise ValueError("Use the format: <code>start - end</code>")
        a, b = int(parts[0].strip()), int(parts[1].strip())
        if a > b:
            raise ValueError("start must be ≤ end")
        return a, b
    if not raw.isdigit():
        raise ValueError("Send <code>0</code>, a message id, or <code>start - end</code>")
    return int(raw) + 1, None


# ── destination picker (paginated, via client.listen — no new file/handler needed) ──
async def _pick_destination(client: Client, uid: int, chat_id: int, channels: list, source_title: str):
    page = 0
    msg = await client.send_message(chat_id, "⏳ Loading destination channels…")

    while True:
        total_pages = max(1, math.ceil(len(channels) / CHANNELS_PER_PAGE))
        page = max(0, min(page, total_pages - 1))
        page_channels = channels[page * CHANNELS_PER_PAGE: page * CHANNELS_PER_PAGE + CHANNELS_PER_PAGE]

        kb = [
            [InlineKeyboardButton(f"📢 {c['title']}", callback_data=f"mf_pick_{c['chat_id']}")]
            for c in page_channels
        ]
        if len(channels) > CHANNELS_PER_PAGE:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️", callback_data="mf_pg_prev"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="mf_noop"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton("➡️", callback_data="mf_pg_next"))
            kb.append(nav)
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="mf_pick_cancel")])

        await msg.edit_text(
            f"✅ Source: <b>{source_title}</b>\n\n"
            f"📥 <b>Select DESTINATION channel</b>  (page {page + 1}/{total_pages})",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )

        try:
            query = await client.listen(
                filters=filters.user(uid) & filters.regex(r"^mf_(pick_|pg_prev$|pg_next$|noop$)"),
                listener_type=ListenerTypes.CALLBACK_QUERY,
                chat_id=chat_id,
                timeout=300,
            )
        except asyncio.TimeoutError:
            await msg.edit_text("⏰ Timed out.")
            return None

        data = query.data
        await query.answer()

        if data == "mf_pick_cancel":
            await msg.edit_text("🛑 Cancelled.")
            return None
        if data == "mf_pg_prev":
            page -= 1
            continue
        if data == "mf_pg_next":
            page += 1
            continue
        if data == "mf_noop":
            continue
        if data.startswith("mf_pick_"):
            dst = int(data[len("mf_pick_"):])
            title = next((c["title"] for c in channels if c["chat_id"] == dst), str(dst))
            return dst, title


# ── scan (batched get_messages) + bulk enqueue ────────────────────────────────
async def _scan_and_enqueue(client: Client, uid: int, session_id: str, src: int, dst: int,
                             start_id: int, end_id):
    s = MF_ACTIVE_SESSIONS.get(uid)
    if not s:
        return

    cur = start_id
    total_files = 0
    consecutive_empty_chunks = 0
    pending_ids: list = []
    batches_to_insert: list = []

    def _flush_pending():
        nonlocal total_files
        while len(pending_ids) >= FORWARD_BATCH:
            chunk = pending_ids[:FORWARD_BATCH]
            del pending_ids[:FORWARD_BATCH]
            batches_to_insert.append({
                "session_id": session_id, "user_id": uid, "src": src, "dst": dst,
                "msg_ids": chunk,
                "chat_id": s["chat_id"], "ui_msg": s["msg_id"],
                "source_title": s["source_title"], "destination_title": s["destination_title"],
            })
            total_files += len(chunk)

    while True:
        if end_id is not None and cur > end_id:
            break
        if session_id in MF_CANCELLED_SESSIONS:
            return

        chunk_end = cur + SCAN_CHUNK - 1
        if end_id is not None:
            chunk_end = min(chunk_end, end_id)
        ids = list(range(cur, chunk_end + 1))

        try:
            msgs = await user_client.get_messages(src, ids)
        except FloodWait as e:
            wait = int(e.value) + 2
            logger.info(f"[MF_SCAN] FloodWait {wait}s on {src}")
            await asyncio.sleep(wait)
            continue  # retry same chunk
        except Exception as e:
            logger.warning(f"[MF_SCAN] get_messages error on {src} [{cur}:{chunk_end}]: {e}")
            msgs = []

        if not isinstance(msgs, list):
            msgs = [msgs] if msgs else []

        any_found = False
        for m in msgs:
            if m and not getattr(m, "empty", True):
                any_found = True
                if m.media:
                    pending_ids.append(m.id)
        _flush_pending()

        if any_found:
            consecutive_empty_chunks = 0
        else:
            consecutive_empty_chunks += 1
            if end_id is None and consecutive_empty_chunks >= MAX_EMPTY_CHUNKS:
                break

        if batches_to_insert:
            await db.enqueue_member_forward_batches(batches_to_insert)
            batches_to_insert = []

        cur = chunk_end + 1
        await asyncio.sleep(0)

    if pending_ids:
        batches_to_insert.append({
            "session_id": session_id, "user_id": uid, "src": src, "dst": dst,
            "msg_ids": pending_ids,
            "chat_id": s["chat_id"], "ui_msg": s["msg_id"],
            "source_title": s["source_title"], "destination_title": s["destination_title"],
        })
        total_files += len(pending_ids)
    if batches_to_insert:
        await db.enqueue_member_forward_batches(batches_to_insert)

    s["total_files"] = total_files
    s["scan_done"] = True

    if session_id in MF_CANCELLED_SESSIONS:
        return
    if total_files == 0:
        MF_ACTIVE_SESSIONS.pop(uid, None)
        try:
            await client.edit_message_text(
                s["chat_id"], s["msg_id"],
                f"✅ Scan complete — no files found in <b>{s['source_title']}</b> for that range.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    try:
        await client.edit_message_text(
            s["chat_id"], s["msg_id"],
            (
                f"📤 <b>{s['source_title']}</b>\n"
                f"         ⬇️⬇️⬇️\n"
                f"📥 <b>{s['destination_title']}</b>\n\n"
                f"🔄 Forwarding {total_files} file(s)…"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_progress_markup(session_id),
        )
    except Exception:
        pass


def _progress_markup(session_id: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"mf_cancel_{session_id}")]])


async def _update_progress_markup(client: Client, uid: int):
    s = MF_ACTIVE_SESSIONS.get(uid)
    if not s:
        return
    try:
        await client.edit_message_reply_markup(s["chat_id"], s["msg_id"], _progress_markup(s["session_id"]))
    except Exception:
        pass


# ── forward workers (batched — the actual speed win) ──────────────────────────
async def _member_forward_worker(bot_client: Client):
    while True:
        try:
            batch = await db.member_forward_claim_one()
        except Exception as e:
            logger.warning(f"[MF_WORKER] claim error: {e}")
            await asyncio.sleep(2)
            continue
        if not batch:
            await asyncio.sleep(1)
            continue

        session_id = batch.get("session_id")
        src, dst, ids = batch["src"], batch["dst"], batch["msg_ids"]

        try:
            if session_id in MF_CANCELLED_SESSIONS:
                await db.member_forward_done(batch["_id"])
                continue

            try:
                await user_client.forward_messages(chat_id=dst, from_chat_id=src, message_ids=ids, drop_author=True)
            except FloodWait as e:
                wait = min(300, int(e.value) + 2 + (2 ** min(batch.get("retries", 0), 6)))
                logger.info(f"[MF_WORKER] FloodWait {wait}s on ({src}->{dst}), retrying batch of {len(ids)}")
                await db.member_forward_retry(batch["_id"], wait)
                continue
            except Exception as e:
                logger.info(f"[MF_BATCH_FAIL] {src}->{dst} batch of {len(ids)}: {e} — falling back one-by-one")
                await _forward_one_by_one(bot_client, src, dst, ids)

            await db.member_forward_done(batch["_id"])
            await _bump_progress(bot_client, batch, len(ids))
            await asyncio.sleep(MEMBER_FORWARD_DELAY)

        except Exception as e:
            logger.warning(f"[MF_WORKER_ERR] {e}")
            await db.member_forward_done(batch["_id"])


async def _forward_one_by_one(bot_client: Client, src: int, dst: int, ids: list):
    """Fallback for a batch that failed as a whole — isolates whichever ids
    are actually the problem instead of losing the whole batch. Tries the
    same fast drop_author forward per id first; only downloads+re-uploads
    if that genuinely can't reach the destination."""
    for mid in ids:
        try:
            await user_client.forward_messages(chat_id=dst, from_chat_id=src, message_ids=mid, drop_author=True)
            continue
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + 1)
            try:
                await user_client.forward_messages(chat_id=dst, from_chat_id=src, message_ids=mid, drop_author=True)
                continue
            except Exception:
                pass
        except Exception as e:
            logger.info(f"[MF_SINGLE_FAIL] {mid}: {e} — trying download+re-upload")

        try:
            msg = await user_client.get_messages(src, mid)
            if not msg or getattr(msg, "empty", True) or not msg.media:
                continue
            await _download_reupload(bot_client, dst, msg)
        except Exception as e:
            logger.warning(f"[MF_FALLBACK_FAIL] {mid}: {e}")


async def _download_reupload(bot_client: Client, dst: int, msg) -> None:
    tmp_path = None
    try:
        tmp_path = await user_client.download_media(msg, file_name=f"/tmp/mf_{uuid.uuid4().hex}_")
        if not tmp_path:
            return
        caption = msg.caption.html if msg.caption else ""
        if msg.video:
            v = msg.video
            await bot_client.send_video(
                dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML,
                duration=getattr(v, "duration", 0), width=getattr(v, "width", 0),
                height=getattr(v, "height", 0), supports_streaming=True,
            )
        elif msg.animation:
            await bot_client.send_animation(dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML)
        elif msg.audio:
            await bot_client.send_audio(dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML)
        elif msg.voice:
            await bot_client.send_voice(dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML)
        elif msg.photo:
            await bot_client.send_photo(dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML)
        else:
            await bot_client.send_document(dst, tmp_path, caption=caption, parse_mode=ParseMode.HTML)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


async def _bump_progress(bot_client: Client, batch: dict, files_done: int):
    session_id = batch.get("session_id")
    if not session_id or session_id in MF_CANCELLED_SESSIONS:
        return
    uid = batch.get("user_id")
    s = MF_ACTIVE_SESSIONS.get(uid) if uid else None
    if not s or s.get("session_id") != session_id:
        return  # stale batch from a session that already finished/cancelled

    s["done_files"] = s.get("done_files", 0) + files_done
    done, total, scan_done = s["done_files"], s.get("total_files", 0), s.get("scan_done", False)

    if scan_done and total > 0 and done >= total:
        MF_ACTIVE_SESSIONS.pop(uid, None)
        try:
            await bot_client.edit_message_text(
                batch["chat_id"], batch["ui_msg"],
                (
                    "✅ <b>Member-channel forwarding completed</b>\n\n"
                    f"📤 <b>Source:</b> {batch['source_title']}\n"
                    f"📥 <b>Destination:</b> {batch['destination_title']}\n\n"
                    f"📦 <b>Files forwarded:</b> <code>{done}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    # Throttle edits — Telegram rate-limits message edits too.
    if done % PROGRESS_EVERY_FILES > files_done:
        return

    pct = int((done / total) * 100) if total > 0 else 0
    bar_fill = pct // 10
    bar = "▓" * bar_fill + "░" * (10 - bar_fill)
    try:
        await bot_client.edit_message_text(
            batch["chat_id"], batch["ui_msg"],
            (
                f"📤 <b>{batch['source_title']}</b>\n"
                f"         ⬇️⬇️⬇️\n"
                f"📥 <b>{batch['destination_title']}</b>\n\n"
                f"🔄 Forwarding…\n"
                f"[{bar}] <code>{pct}%</code>\n"
                f"📦 <b>Done:</b> <code>{done}</code> / <code>{total if total else '?'}</code>"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_progress_markup(session_id),
        )
    except Exception:
        pass


# ── cancel (works anytime, independent of the linear ask()/listen() flow) ────
@Client.on_callback_query(filters.regex(r"^mf_cancel_"))
async def mf_cancel_cb(client: Client, query):
    session_id = query.data[len("mf_cancel_"):]
    uid = query.from_user.id
    s = MF_ACTIVE_SESSIONS.get(uid)

    MF_CANCELLED_SESSIONS.add(session_id)
    deleted = await db.member_forward_delete_session(session_id)

    if s and s.get("session_id") == session_id:
        MF_ACTIVE_SESSIONS.pop(uid, None)
        done = s.get("done_files", 0)
        await query.message.edit_text(
            f"🛑 <b>Member-channel forwarding cancelled</b>\n\n"
            f"📦 <b>Files forwarded:</b> <code>{done}</code>\n"
            f"🗑 <b>Queued batches dropped:</b> <code>{deleted}</code>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await query.message.edit_text("🛑 Cancelled.")
    await query.answer()
