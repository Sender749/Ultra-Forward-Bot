#Dont Remove My Credit @Silicon_Bot_Update 
#This Repo Is By @Silicon_Official 
# For Any Kind Of Error Ask Us In Support Group @Silicon_Botz 

"""
plugins/member_forward.py — /member_forward

Forwards files out of a channel the BOT ITSELF isn't in, but a personal
Telegram account (logged in via SESSION_STRING, see user_session.py) is —
into any channel you've already registered via /settings → channels.

UI matches the captionbot repo's /member_forward: a session dict per user
driving inline-button steps (source → destination picker → range → live
progress), NOT client.ask()/client.listen(). The earlier ask()/listen()
version turned out to be a real risk: if the bot didn't respond to
/member_forward at all, the most likely causes were (a) BOT_OWNER_ID not
set to your actual Telegram ID — see the diagnostic handler at the bottom
of this file and of gen_session.py, which now tells you your ID directly
in chat instead of staying silent — and/or (b) relying on this pyrofork
build's conversation-helper behaving a certain way. This version removes
cause (b) entirely by using the same plain handlers as every other command
in this repo.

Built for speed
────────────────────────────────────────────────────────────────────────────
  • SCANS in batches — one get_messages() call fetches up to 100 message
    ids at once, instead of one call per id.
  • FORWARDS in batches — forward_messages(..., drop_author=True) moves up
    to 100 messages in ONE Telegram API call, with no visible "Forwarded
    from" tag. No per-file sleep in the common case.
  • Falls back to one-by-one only for whichever ids fail inside a batch,
    and to download+re-upload only if the userbot can't write to the
    destination at all.

Progress reporting is time-throttled (edits roughly every few seconds, not
every file — Telegram rate-limits message edits too) and shows a live
scan count, done/total, percent, elapsed time, speed, and ETA.

Isolated from every other feature: its own Mongo collection
(member_forward_queue, see database.py), its own worker pool, its own
userbot connection (user_session.py).
"""

import asyncio
import logging
import math
import os
import time
import uuid

from pyrogram import Client, enums, filters, ContinuePropagation
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from database import db
from user_session import user_client, USER_SESSION_ENABLED

logger = logging.getLogger(__name__)

_ADMIN_FILTER = filters.user(Config.BOT_OWNER_ID)

SCAN_CHUNK            = 100   # message ids fetched per get_messages() call
FORWARD_BATCH         = 100   # message ids forwarded per forward_messages() call
CHANNELS_PER_PAGE     = 10
MEMBER_FORWARD_WORKERS = 3
MEMBER_FORWARD_DELAY   = 0.4   # cooldown per worker between batch sends (not per-file!)
MAX_EMPTY_CHUNKS       = 5     # 5 × SCAN_CHUNK = 500 consecutive missing ids ⇒ assume end of channel
PROGRESS_EDIT_INTERVAL = 3.0   # seconds — throttles message edits (Telegram rate-limits edits too)
SESSION_TTL            = 900   # 15 minutes for each *input* step (source/dst/range)

# uid -> single session dict covering BOTH the input-collection steps
# (await_source / dst / skip) AND the running job's progress/cancel state.
MF_SESSIONS = {}
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


def _fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# ── /member_forward ───────────────────────────────────────────────────────────
@Client.on_message(filters.private & _ADMIN_FILTER & filters.command("member_forward"))
async def member_forward_cmd(client: Client, message):
    if not USER_SESSION_ENABLED:
        return await message.reply_text(
            "❌ <b>Member-channel forwarding isn't set up.</b>\n\n"
            "Generate a session string with /gen_session, set it as the "
            "<code>SESSION_STRING</code> environment variable, then "
            "restart the bot.",
            parse_mode=ParseMode.HTML,
        )

    uid = message.from_user.id
    if uid in MF_SESSIONS:
        return await message.reply_text(
            "❌ You already have a member-forward flow in progress. "
            "Use the ❌ Cancel button on it, or wait for it to finish."
        )

    MF_SESSIONS[uid] = {"step": "await_source", "expires": time.time() + SESSION_TTL}
    await message.reply_text(
        "📡 <b>Member-Channel Forward</b>\n\n"
        "Send the <b>source channel</b> — one your personal account (the "
        "userbot) is a member/admin of, but this bot is <u>not</u> added to.\n\n"
        "You can send:\n"
        "• a <code>@username</code>\n"
        "• a <code>t.me/...</code> link or invite link\n"
        "• a numeric channel ID (e.g. <code>-1001234567890</code>)\n\n"
        "• Session expires in <b>15 minutes</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")]]),
    )


# ── shared owner-text dispatch (MF + GS) — the only broad text handler here ──
@Client.on_message(filters.private & _ADMIN_FILTER & filters.text)
async def _owner_text_dispatch(client: Client, message):
    uid = message.from_user.id

    if uid in MF_SESSIONS:
        await _handle_mf_text(client, message, uid)
        return

    # Delegates to gen_session.py exactly like Caption.py delegates to
    # member_forward.py in the captionbot repo — kept as one shared
    # handler so there's only ONE broad private-text handler in this repo,
    # avoiding any ordering ambiguity between two separate broad handlers.
    from plugins.gen_session import GS_SESSIONS, handle_gen_session_text
    if uid in GS_SESSIONS:
        if await handle_gen_session_text(client, message):
            return

    raise ContinuePropagation()


async def _handle_mf_text(client: Client, message, uid: int):
    s = MF_SESSIONS.get(uid)
    if not s:
        return
    if s.get("expires") and s["expires"] < time.time():
        MF_SESSIONS.pop(uid, None)
        await message.reply_text("⏰ Session expired.\nStart again using /member_forward")
        return

    step = s.get("step")
    if step == "await_source":
        await _handle_source_input(client, message, uid, s)
    elif step == "skip":
        await _handle_range_input(client, message, uid, s)
    # step == "running" (or anything else): background job owns the UI now,
    # nothing to do with free text at that point.


async def _handle_source_input(client: Client, message, uid: int, s: dict):
    raw = (message.text or "").strip()
    if not raw:
        return
    try:
        chat = await user_client.get_chat(raw)
    except FloodWait as e:
        await message.reply_text(
            f"⏳ Telegram is rate-limiting the userbot account. Wait "
            f"<b>{int(e.value)}s</b> and send the channel again.",
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception as e:
        await message.reply_text(
            f"❌ Couldn't access that channel with the userbot account.\n"
            f"<code>{e}</code>\n\n"
            "Make sure the personal account behind SESSION_STRING is a "
            "member there, then try again.",
            parse_mode=ParseMode.HTML,
        )
        return

    if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
        await message.reply_text("❌ That's not a channel/supergroup.")
        return

    s["source"] = chat.id
    s["source_title"] = chat.title or str(chat.id)
    s["step"] = "dst"
    s["expires"] = time.time() + SESSION_TTL

    channels = await db.get_user_channels(uid)
    channels = [c for c in channels if c["chat_id"] != chat.id]
    if not channels:
        MF_SESSIONS.pop(uid, None)
        await message.reply_text(
            "❌ No destination channels registered.\n\n"
            "Add one first: /settings → channels → ✚ Add Channel."
        )
        return

    s["dst_channels"] = channels
    s["dst_page"] = 0
    picker_msg = await message.reply_text("⏳ Loading channels…")
    s["chat_id"] = picker_msg.chat.id
    s["msg_id"] = picker_msg.id
    await _render_mf_dst_picker(client, s)


# ── destination picker (paginated, from your /settings → channels list) ──────
async def _render_mf_dst_picker(client: Client, s: dict):
    channels = s.get("dst_channels", [])
    if not channels:
        await client.edit_message_text(s["chat_id"], s["msg_id"], "❌ No destination channels available.")
        return

    total_pages = max(1, math.ceil(len(channels) / CHANNELS_PER_PAGE))
    page = max(0, min(s.get("dst_page", 0), total_pages - 1))
    s["dst_page"] = page
    page_channels = channels[page * CHANNELS_PER_PAGE: page * CHANNELS_PER_PAGE + CHANNELS_PER_PAGE]

    kb = [
        [InlineKeyboardButton(f"📢 {c['title']}", callback_data=f"mf_dst_sel_{c['chat_id']}")]
        for c in page_channels
    ]
    if len(channels) > CHANNELS_PER_PAGE:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"mf_dst_pg_{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="mf_noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"mf_dst_pg_{page + 1}"))
        kb.append(nav)
    kb.append([InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")])

    await client.edit_message_text(
        s["chat_id"], s["msg_id"],
        f"✅ Source: <b>{s['source_title']}</b>\n\n"
        f"📥 <b>Select DESTINATION channel</b>  (page {page + 1}/{total_pages})",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb),
    )


@Client.on_callback_query(filters.regex(r"^mf_dst_pg_(\d+)$"))
async def mf_dst_page(client: Client, query):
    uid = query.from_user.id
    s = MF_SESSIONS.get(uid)
    if not s:
        return await query.answer()
    s["dst_page"] = int(query.matches[0].group(1))
    await query.answer()
    await _render_mf_dst_picker(client, s)


@Client.on_callback_query(filters.regex(r"^mf_dst_sel_(-?\d+)$"))
async def mf_dst_sel(client: Client, query):
    uid = query.from_user.id
    s = MF_SESSIONS.get(uid)
    if not s:
        return await query.answer()
    dst = int(query.matches[0].group(1))
    title = next((c["title"] for c in s.get("dst_channels", []) if c["chat_id"] == dst), str(dst))

    s["destination"] = dst
    s["destination_title"] = title
    s["step"] = "skip"
    s["expires"] = time.time() + SESSION_TTL
    await query.answer()
    await client.edit_message_text(
        s["chat_id"], s["msg_id"],
        "⏭ <b>Forwarding range</b>\n\n"
        "• <code>0</code> — forward ALL files\n"
        "• <code>&lt;id&gt;</code> — start AFTER this message id\n"
        "• <code>start - end</code> — forward BETWEEN two ids (inclusive)\n\n"
        "• Session expires in <b>15 minutes</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")]]),
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


async def _handle_range_input(client: Client, message, uid: int, s: dict):
    try:
        start_id, end_id = _parse_range(message.text or "")
    except ValueError as e:
        await message.reply_text(f"❌ {e}", parse_mode=ParseMode.HTML)
        return

    s["start_id"] = start_id
    s["end_id"] = end_id
    s["step"] = "running"
    session_id = str(uuid.uuid4())
    s["session_id"] = session_id
    s["total_files"] = 0
    s["done_files"] = 0
    s["scanned_count"] = 0
    s["scan_done"] = False
    s["scan_started_ts"] = time.time()
    s["forward_started_ts"] = None
    s["last_progress_edit_ts"] = 0

    try:
        await message.delete()
    except Exception:
        pass
    try:
        await client.edit_message_text(
            s["chat_id"], s["msg_id"],
            f"📤 <b>{s['source_title']}</b>\n"
            f"         ⬇️⬇️⬇️\n"
            f"📥 <b>{s['destination_title']}</b>\n\n"
            f"🔍 Scanning…",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")]]),
        )
    except Exception:
        pass

    asyncio.create_task(
        _scan_and_enqueue(client, uid, session_id, s["source"], s["destination"], start_id, end_id),
        name=f"mf_scan_{uid}_{session_id[:8]}",
    )


# ── scan (batched get_messages) + bulk enqueue, with live progress ───────────
async def _scan_and_enqueue(client: Client, uid: int, session_id: str, src: int, dst: int,
                             start_id: int, end_id):
    s = MF_SESSIONS.get(uid)
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

        s["scanned_count"] = total_files + len(pending_ids)
        await _maybe_edit_scan_progress(client, s, cur)

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
    s["forward_started_ts"] = time.time()

    if session_id in MF_CANCELLED_SESSIONS:
        return

    if total_files == 0:
        MF_SESSIONS.pop(uid, None)
        try:
            await client.edit_message_text(
                s["chat_id"], s["msg_id"],
                f"✅ Scan complete — no files found in <b>{s['source_title']}</b> for that range.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    await _render_progress(client, s, force=True)


async def _maybe_edit_scan_progress(client: Client, s: dict, up_to_id: int):
    now = time.time()
    if now - s.get("last_progress_edit_ts", 0) < PROGRESS_EDIT_INTERVAL:
        return
    s["last_progress_edit_ts"] = now
    elapsed = now - s["scan_started_ts"]
    try:
        await client.edit_message_text(
            s["chat_id"], s["msg_id"],
            f"📤 <b>{s['source_title']}</b>\n"
            f"         ⬇️⬇️⬇️\n"
            f"📥 <b>{s['destination_title']}</b>\n\n"
            f"🔍 Scanning… found <code>{s['scanned_count']}</code> file(s) so far\n"
            f"⏱ <code>{_fmt_duration(elapsed)}</code> elapsed (up to message id {up_to_id})",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")]]),
        )
    except Exception:
        pass


def _progress_text(s: dict) -> str:
    done, total = s.get("done_files", 0), s.get("total_files", 0)
    pct = int((done / total) * 100) if total > 0 else 0
    bar_fill = pct // 10
    bar = "▓" * bar_fill + "░" * (10 - bar_fill)

    elapsed = time.time() - (s.get("forward_started_ts") or time.time())
    rate = done / elapsed if elapsed > 0 else 0
    remaining = max(0, total - done)
    eta = _fmt_duration(remaining / rate) if rate > 0 else "…"
    speed = f"{rate:.1f}/s" if rate > 0 else "…"

    return (
        f"📤 <b>{s['source_title']}</b>\n"
        f"         ⬇️⬇️⬇️\n"
        f"📥 <b>{s['destination_title']}</b>\n\n"
        f"🔄 Forwarding…\n"
        f"[{bar}] <code>{pct}%</code>\n"
        f"📦 <b>Done:</b> <code>{done}</code> / <code>{total if total else '?'}</code>\n"
        f"⚡ <b>Speed:</b> <code>{speed}</code>   ⏱ <b>Elapsed:</b> <code>{_fmt_duration(elapsed)}</code>\n"
        f"⏳ <b>ETA:</b> <code>{eta}</code>"
    )


async def _render_progress(client: Client, s: dict, force: bool = False):
    now = time.time()
    if not force and now - s.get("last_progress_edit_ts", 0) < PROGRESS_EDIT_INTERVAL:
        return
    s["last_progress_edit_ts"] = now
    try:
        await client.edit_message_text(
            s["chat_id"], s["msg_id"], _progress_text(s),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="mf_cancel")]]),
        )
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
    are actually the problem instead of losing the whole batch."""
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
    s = MF_SESSIONS.get(uid) if uid else None
    if not s or s.get("session_id") != session_id:
        return  # stale batch from a session that already finished/cancelled

    s["done_files"] = s.get("done_files", 0) + files_done
    done, total, scan_done = s["done_files"], s.get("total_files", 0), s.get("scan_done", False)

    if scan_done and total > 0 and done >= total:
        MF_SESSIONS.pop(uid, None)
        elapsed = time.time() - (s.get("forward_started_ts") or time.time())
        rate = done / elapsed if elapsed > 0 else 0
        try:
            await bot_client.edit_message_text(
                batch["chat_id"], batch["ui_msg"],
                (
                    "✅ <b>Member-channel forwarding completed</b>\n\n"
                    f"📤 <b>Source:</b> {batch['source_title']}\n"
                    f"📥 <b>Destination:</b> {batch['destination_title']}\n\n"
                    f"📦 <b>Files forwarded:</b> <code>{done}</code>\n"
                    f"⏱ <b>Time taken:</b> <code>{_fmt_duration(elapsed)}</code>\n"
                    f"⚡ <b>Avg speed:</b> <code>{rate:.1f} files/s</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    await _render_progress(bot_client, s)


# ── cancel (works anytime — during input steps AND during the running job) ───
@Client.on_callback_query(filters.regex(r"^mf_cancel$"))
async def mf_cancel_cb(client: Client, query):
    uid = query.from_user.id
    s = MF_SESSIONS.pop(uid, None)
    if not s:
        return await query.message.edit_text("❌ Nothing to cancel.")

    session_id = s.get("session_id")
    if session_id:
        MF_CANCELLED_SESSIONS.add(session_id)
        deleted = await db.member_forward_delete_session(session_id)
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


@Client.on_callback_query(filters.regex(r"^mf_noop$"))
async def mf_noop_cb(client: Client, query):
    await query.answer()


# ── diagnostic: fires ONLY for non-owners, self-explains the silent-ignore ──
@Client.on_message(filters.private & filters.command("member_forward") & ~_ADMIN_FILTER)
async def member_forward_not_owner(client: Client, message):
    await message.reply_text(
        "❌ This command is restricted to the bot owner.\n\n"
        f"Your Telegram ID is <code>{message.from_user.id}</code>. If this "
        f"should be allowed, add it to the <code>BOT_OWNER_ID</code> "
        f"environment variable and restart the bot.",
        parse_mode=ParseMode.HTML,
    )
