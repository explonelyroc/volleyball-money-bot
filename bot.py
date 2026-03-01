import os
import math
import sqlite3
from datetime import datetime, date
from typing import Optional, Tuple, List, Dict

from telegram import (
    Update,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PollAnswerHandler, CallbackQueryHandler,
    MessageHandler, filters
)

DB_PATH = os.environ.get("DB_PATH", "bot.db")

PAGE_SIZE = 10
MONTHS_PAGE_SIZE = 8

MODE_ALL = "all"
MODE_UNPAID = "unpaid"
MODE_PAID = "paid"

# States for button-driven input
S_NONE = "none"
S_ABON = "abon"           # steps: amount -> year -> month
S_GAME = "game"           # step: name
S_GAMES_TOTAL = "gtotal"  # steps: amount -> year -> month

RU_MONTHS = {
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4, "май": 5, "июнь": 6,
    "июль": 7, "август": 8, "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}
NUM_TO_RU = {v: k for k, v in RU_MONTHS.items()}


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def make_month_key(year: int, month_num: int) -> str:
    return f"{year:04d}-{month_num:02d}"


def month_key_to_label(month_key: str) -> str:
    # "2026-03" -> "2026 март"
    y = int(month_key[:4])
    m = int(month_key[5:7])
    return f"{y} {NUM_TO_RU.get(m, str(m))}"


def ceil_div(total: int, n: int) -> Optional[int]:
    if n <= 0:
        return None
    return math.ceil(total / n)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript("""
        -- Абонемент
        CREATE TABLE IF NOT EXISTS polls (
            poll_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            poll_message_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            month_key TEXT NOT NULL,
            year INTEGER NOT NULL,
            month_num INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'PLN',
            total_amount INTEGER NOT NULL,
            yes_votes INTEGER NOT NULL DEFAULT 0,
            final_per_person INTEGER,
            created_at TEXT NOT NULL,
            closed_at TEXT,
            is_active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS votes (
            poll_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            choice INTEGER,                   -- 0=✅,1=❌,NULL=cancel vote
            updated_at TEXT NOT NULL,
            PRIMARY KEY (poll_id, user_id),
            FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS payments (
            poll_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            paid_at TEXT NOT NULL,
            PRIMARY KEY (poll_id, user_id),
            FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE
        );

        -- Users
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_polls_chat_active ON polls(chat_id, is_active);
        CREATE INDEX IF NOT EXISTS idx_polls_chat_month ON polls(chat_id, month_key);

        -- ===== Разовые игры =====
        CREATE TABLE IF NOT EXISTS game_polls (
            game_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            poll_id TEXT UNIQUE NOT NULL,
            poll_message_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            game_date TEXT NOT NULL,        -- YYYY-MM-DD
            month_key TEXT NOT NULL,        -- YYYY-MM
            currency TEXT NOT NULL DEFAULT 'PLN',
            yes_votes INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            closed_at TEXT,
            is_active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS game_votes (
            poll_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            choice INTEGER,                 -- 0=✅,1=❌,NULL=cancel
            updated_at TEXT NOT NULL,
            PRIMARY KEY (poll_id, user_id),
            FOREIGN KEY (poll_id) REFERENCES game_polls(poll_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS games_month_totals (
            chat_id INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            total_amount_pln INTEGER NOT NULL,
            computed_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, month_key)
        );

        CREATE TABLE IF NOT EXISTS games_day_charges (
            chat_id INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            game_date TEXT NOT NULL,
            day_share_grosz INTEGER NOT NULL,
            yes_count INTEGER NOT NULL,
            per_person_pln INTEGER,
            computed_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, month_key, game_date)
        );

        CREATE TABLE IF NOT EXISTS games_user_charges (
            chat_id INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            amount_pln INTEGER NOT NULL,
            computed_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, month_key, user_id)
        );

        CREATE TABLE IF NOT EXISTS games_payments (
            chat_id INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            paid_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, month_key, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_game_polls_chat_month ON game_polls(chat_id, month_key);
        """)


def upsert_user(user) -> None:
    with db() as conn:
        conn.execute("""
            INSERT INTO users(user_id, username, first_name, last_name, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name,
              last_name=excluded.last_name,
              updated_at=excluded.updated_at
        """, (user.id, user.username, user.first_name, user.last_name, now_iso()))


def display_name_expr() -> str:
    return """
    COALESCE(
      NULLIF(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), ''),
      COALESCE(NULLIF(u.username,''), 'user')
    ) AS display_name
    """


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user:
        return False
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def admin_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("🧾 Абонемент"), KeyboardButton("✅ Закрыть абонемент")],
        [KeyboardButton("🎮 Игра"), KeyboardButton("✅ Закрыть игру")],
        [KeyboardButton("💰 Сумма игр за месяц"), KeyboardButton("📆 Месяцы")],
        [KeyboardButton("📌 Мои долги"), KeyboardButton("ℹ️ Помощь")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def user_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("📌 Мои долги"), KeyboardButton("ℹ️ Помощь")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def parse_poll_args(args: List[str]) -> Tuple[int, int, int, str]:
    # /poll 1400 2026 март  OR /poll 1400 март 2026
    if len(args) < 3:
        raise ValueError("Нужно: /poll <сумма> <год> <месяц-словом> (или /poll <сумма> <месяц> <год>)")

    try:
        amount = int("".join(ch for ch in args[0] if ch.isdigit()))
        if amount <= 0:
            raise ValueError
    except Exception:
        raise ValueError("Сумма должна быть целым числом, например: /poll 1400 2026 март")

    a1 = args[1].strip().lower()
    a2 = args[2].strip().lower()

    if a1.isdigit() and a2 in RU_MONTHS:
        year = int(a1)
        month_name = a2
        month_num = RU_MONTHS[month_name]
        return amount, year, month_num, month_name

    if a1 in RU_MONTHS and a2.isdigit():
        month_name = a1
        month_num = RU_MONTHS[month_name]
        year = int(a2)
        return amount, year, month_num, month_name

    raise ValueError("Формат: /poll 1400 2026 март (или /poll 1400 март 2026). Месяц — словом.")


def parse_month_and_amount_args(args: List[str]) -> Tuple[str, int]:
    """
    /games_total 900 2026 март
    /games_total 900 март 2026
    /games_total 2026 март 900
    """
    if len(args) < 3:
        raise ValueError("Нужно: /games_total <сумма> <год> <месяц> (или /games_total <год> <месяц> <сумма>)")

    amount_idx = None
    amount_val = None
    for i in range(3):
        digits = "".join(ch for ch in args[i] if ch.isdigit())
        if digits:
            val = int(digits)
            if val > 0:
                amount_idx = i
                amount_val = val
                break
    if amount_idx is None or amount_val is None:
        raise ValueError("Не вижу сумму. Пример: /games_total 900 2026 март")

    other = [args[i].strip().lower() for i in range(3) if i != amount_idx]
    a, b = other[0], other[1]
    if a.isdigit() and b in RU_MONTHS:
        year = int(a)
        month_num = RU_MONTHS[b]
    elif a in RU_MONTHS and b.isdigit():
        year = int(b)
        month_num = RU_MONTHS[a]
    else:
        raise ValueError("Не понял месяц. Пример: /games_total 900 2026 март")

    return make_month_key(year, month_num), amount_val


# ---------------- Абонемент: helpers ----------------

def get_latest_active_abon(chat_id: int) -> Optional[tuple]:
    with db() as conn:
        return conn.execute("""
            SELECT poll_id, chat_id, poll_message_id, title, month_key, currency, total_amount, yes_votes, final_per_person, is_active
            FROM polls
            WHERE chat_id=? AND is_active=1
            ORDER BY created_at DESC
            LIMIT 1
        """, (chat_id,)).fetchone()


def get_abon_by_id(poll_id: str) -> Optional[tuple]:
    with db() as conn:
        return conn.execute("""
            SELECT poll_id, chat_id, poll_message_id, title, month_key, currency, total_amount, yes_votes, final_per_person, is_active
            FROM polls WHERE poll_id=?
        """, (poll_id,)).fetchone()


def per_person_abon(poll_row: tuple) -> Optional[int]:
    _, _, _, _, _, _, total_amount, yes_votes, final_pp, is_active = poll_row
    if int(is_active) == 0 and final_pp is not None:
        return int(final_pp)
    return ceil_div(int(total_amount), int(yes_votes))


def count_paid_unpaid_abon(poll_id: str) -> Tuple[int, int]:
    with db() as conn:
        paid = conn.execute("""
            SELECT COUNT(*)
            FROM votes v
            JOIN payments p ON p.poll_id=v.poll_id AND p.user_id=v.user_id
            WHERE v.poll_id=? AND v.choice=0
        """, (poll_id,)).fetchone()[0]
        unpaid = conn.execute("""
            SELECT COUNT(*)
            FROM votes v
            LEFT JOIN payments p ON p.poll_id=v.poll_id AND p.user_id=v.user_id
            WHERE v.poll_id=? AND v.choice=0 AND p.user_id IS NULL
        """, (poll_id,)).fetchone()[0]
    return int(paid), int(unpaid)


def abon_set_paid(poll_id: str, user_id: int) -> None:
    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO payments(poll_id, user_id, paid_at)
            VALUES(?,?,?)
        """, (poll_id, user_id, now_iso()))


def abon_unset_paid(poll_id: str, user_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM payments WHERE poll_id=? AND user_id=?", (poll_id, user_id))


def abon_people_text_and_kb(poll_row: tuple, mode: str, page: int) -> Tuple[str, InlineKeyboardMarkup]:
    poll_id, _, _, title, _, currency, total_amount, yes_votes, final_pp, is_active = poll_row
    pp = per_person_abon(poll_row)
    pp_txt = f"{pp} {currency}" if pp is not None else "—"
    paid_cnt, unpaid_cnt = count_paid_unpaid_abon(poll_id)

    where_paid = ""
    if mode == MODE_UNPAID:
        where_paid = "AND pay.user_id IS NULL"
    elif mode == MODE_PAID:
        where_paid = "AND pay.user_id IS NOT NULL"

    with db() as conn:
        rows = conn.execute(f"""
            SELECT u.user_id, {display_name_expr()},
                   CASE WHEN pay.user_id IS NULL THEN 0 ELSE 1 END AS is_paid
            FROM votes v
            JOIN users u ON u.user_id=v.user_id
            LEFT JOIN payments pay ON pay.poll_id=v.poll_id AND pay.user_id=v.user_id
            WHERE v.poll_id=? AND v.choice=0
            {where_paid}
            ORDER BY is_paid ASC, LOWER(display_name) ASC
        """, (poll_id,)).fetchall()

    total = len(rows)
    if page < 0:
        page = 0
    max_page = max(0, (total - 1) // PAGE_SIZE) if total else 0
    if page > max_page:
        page = max_page

    start = page * PAGE_SIZE
    slice_rows = rows[start:start + PAGE_SIZE]

    status = "🟢 активен" if int(is_active) == 1 else "⚪️ закрыт"
    mode_label = {"all": "Все", "unpaid": "Не оплатили", "paid": "Оплатили"}[mode]

    header = (
        f"[A] {title} ({status})\n"
        f"💰 Сумма: {total_amount} {currency}\n"
        f"🧮 На человека: {pp_txt}\n"
        f"✅ Оплатили: {paid_cnt} | ❗ Не оплатили: {unpaid_cnt}\n"
        f"📋 Режим: {mode_label}\n\n"
    )

    mode_row = [
        InlineKeyboardButton("Все", callback_data=f"aview|{poll_id}|{MODE_ALL}|0"),
        InlineKeyboardButton("Не оплатили", callback_data=f"aview|{poll_id}|{MODE_UNPAID}|0"),
        InlineKeyboardButton("Оплатили", callback_data=f"aview|{poll_id}|{MODE_PAID}|0"),
    ]

    if not rows:
        kb = InlineKeyboardMarkup([
            mode_row,
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"aview|{poll_id}|{mode}|{page}")]
        ])
        return header + "Пока никого.", kb

    lines: List[str] = []
    buttons: List[List[InlineKeyboardButton]] = [mode_row]

    for i, (uid, name, is_paid_flag) in enumerate(slice_rows, start=start + 1):
        is_paid_flag = int(is_paid_flag)
        mark = "✅" if is_paid_flag == 1 else "❗"
        lines.append(f"{i}. {mark} {name}")
        short = (name[:18] + ("…" if len(name) > 18 else ""))
        if is_paid_flag == 1:
            buttons.append([InlineKeyboardButton(f"↩️ отменить: {short}", callback_data=f"aunpay|{poll_id}|{uid}|{mode}|{page}")])
        else:
            buttons.append([InlineKeyboardButton(f"✅ отметить: {short}", callback_data=f"apay|{poll_id}|{uid}|{mode}|{page}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"aview|{poll_id}|{mode}|{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Стр. {page+1}/{max_page+1}", callback_data="noop"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"aview|{poll_id}|{mode}|{page+1}"))
    buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"aview|{poll_id}|{mode}|{page}")])

    return header + "\n".join(lines), InlineKeyboardMarkup(buttons)


# ---------------- Разовые игры: helpers ----------------

def get_latest_active_game(chat_id: int) -> Optional[tuple]:
    with db() as conn:
        return conn.execute("""
            SELECT chat_id, poll_id, poll_message_id, title, game_date, month_key, yes_votes
            FROM game_polls
            WHERE chat_id=? AND is_active=1
            ORDER BY created_at DESC
            LIMIT 1
        """, (chat_id,)).fetchone()


def games_month_has_any(chat_id: int, month_key: str) -> bool:
    with db() as conn:
        return conn.execute("""
            SELECT 1 FROM game_polls WHERE chat_id=? AND month_key=? LIMIT 1
        """, (chat_id, month_key)).fetchone() is not None


def games_get_month_total(chat_id: int, month_key: str) -> Optional[int]:
    with db() as conn:
        row = conn.execute("""
            SELECT total_amount_pln FROM games_month_totals
            WHERE chat_id=? AND month_key=?
        """, (chat_id, month_key)).fetchone()
    return int(row[0]) if row else None


def games_count_paid_unpaid(chat_id: int, month_key: str) -> Tuple[int, int]:
    with db() as conn:
        paid = conn.execute("""
            SELECT COUNT(*)
            FROM games_user_charges c
            JOIN games_payments p
              ON p.chat_id=c.chat_id AND p.month_key=c.month_key AND p.user_id=c.user_id
            WHERE c.chat_id=? AND c.month_key=? AND c.amount_pln > 0
        """, (chat_id, month_key)).fetchone()[0]

        unpaid = conn.execute("""
            SELECT COUNT(*)
            FROM games_user_charges c
            LEFT JOIN games_payments p
              ON p.chat_id=c.chat_id AND p.month_key=c.month_key AND p.user_id=c.user_id
            WHERE c.chat_id=? AND c.month_key=? AND c.amount_pln > 0 AND p.user_id IS NULL
        """, (chat_id, month_key)).fetchone()[0]
    return int(paid), int(unpaid)


def games_get_people(chat_id: int, month_key: str, mode: str) -> List[Tuple[int, str, int, int]]:
    where_paid = ""
    if mode == MODE_UNPAID:
        where_paid = "AND p.user_id IS NULL"
    elif mode == MODE_PAID:
        where_paid = "AND p.user_id IS NOT NULL"

    with db() as conn:
        rows = conn.execute(f"""
            SELECT u.user_id, {display_name_expr()},
                   c.amount_pln,
                   CASE WHEN p.user_id IS NULL THEN 0 ELSE 1 END AS is_paid
            FROM games_user_charges c
            JOIN users u ON u.user_id=c.user_id
            LEFT JOIN games_payments p
              ON p.chat_id=c.chat_id AND p.month_key=c.month_key AND p.user_id=c.user_id
            WHERE c.chat_id=? AND c.month_key=? AND c.amount_pln > 0
            {where_paid}
            ORDER BY is_paid ASC, c.amount_pln DESC, LOWER(display_name) ASC
        """, (chat_id, month_key)).fetchall()

    return [(int(r[0]), str(r[1]), int(r[2]), int(r[3])) for r in rows]


def games_set_paid(chat_id: int, month_key: str, user_id: int) -> None:
    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO games_payments(chat_id, month_key, user_id, paid_at)
            VALUES(?,?,?,?)
        """, (chat_id, month_key, user_id, now_iso()))


def games_unset_paid(chat_id: int, month_key: str, user_id: int) -> None:
    with db() as conn:
        conn.execute("""
            DELETE FROM games_payments WHERE chat_id=? AND month_key=? AND user_id=?
        """, (chat_id, month_key, user_id))


def games_month_text_and_kb(chat_id: int, month_key: str, mode: str, page: int) -> Tuple[str, InlineKeyboardMarkup]:
    total_pln = games_get_month_total(chat_id, month_key)
    label = month_key_to_label(month_key)
    paid_cnt, unpaid_cnt = games_count_paid_unpaid(chat_id, month_key)
    mode_label = {"all": "Все", "unpaid": "Не оплатили", "paid": "Оплатили"}[mode]

    header = (
        f"[G] Разовые игры {label}\n"
        f"💰 Сумма за месяц (админ): {total_pln if total_pln is not None else '—'} PLN\n"
        f"✅ Оплатили: {paid_cnt} | ❗ Не оплатили: {unpaid_cnt}\n"
        f"📋 Режим: {mode_label}\n\n"
    )

    mode_row = [
        InlineKeyboardButton("Все", callback_data=f"gview|{month_key}|{MODE_ALL}|0"),
        InlineKeyboardButton("Не оплатили", callback_data=f"gview|{month_key}|{MODE_UNPAID}|0"),
        InlineKeyboardButton("Оплатили", callback_data=f"gview|{month_key}|{MODE_PAID}|0"),
    ]

    if total_pln is None:
        kb = InlineKeyboardMarkup([mode_row])
        return header + "Сумма за разовые игры ещё не задана.\nКоманда: /games_total <сумма> <год> <месяц>", kb

    people = games_get_people(chat_id, month_key, mode)
    if not people:
        kb = InlineKeyboardMarkup([mode_row, [InlineKeyboardButton("🔄 Обновить", callback_data=f"gview|{month_key}|{mode}|0")]])
        return header + "Нет долгов (или все оплатили).", kb

    total = len(people)
    if page < 0:
        page = 0
    max_page = max(0, (total - 1) // PAGE_SIZE)
    if page > max_page:
        page = max_page

    start = page * PAGE_SIZE
    slice_people = people[start:start + PAGE_SIZE]

    lines: List[str] = []
    buttons: List[List[InlineKeyboardButton]] = [mode_row]

    for i, (uid, name, amount_pln, is_paid) in enumerate(slice_people, start=start + 1):
        mark = "✅" if is_paid == 1 else "❗"
        lines.append(f"{i}. {mark} {name} — {amount_pln} PLN")
        short = (name[:16] + ("…" if len(name) > 16 else ""))
        if is_paid == 1:
            buttons.append([InlineKeyboardButton(f"↩️ отменить: {short}", callback_data=f"gunpay|{month_key}|{uid}|{mode}|{page}")])
        else:
            buttons.append([InlineKeyboardButton(f"✅ отметить: {short}", callback_data=f"gpay|{month_key}|{uid}|{mode}|{page}")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"gview|{month_key}|{mode}|{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Стр. {page+1}/{max_page+1}", callback_data="noop"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"gview|{month_key}|{mode}|{page+1}"))
    buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"gview|{month_key}|{mode}|{page}")])

    return header + "\n".join(lines), InlineKeyboardMarkup(buttons)


# ---------------- /months menu ----------------

def months_union(chat_id: int) -> List[str]:
    with db() as conn:
        a = conn.execute("SELECT DISTINCT month_key FROM polls WHERE chat_id=?", (chat_id,)).fetchall()
        g = conn.execute("SELECT DISTINCT month_key FROM game_polls WHERE chat_id=?", (chat_id,)).fetchall()
    keys = sorted({r[0] for r in a} | {r[0] for r in g}, reverse=True)
    return keys


def latest_abon_for_month(chat_id: int, month_key: str) -> Optional[tuple]:
    with db() as conn:
        return conn.execute("""
            SELECT poll_id, title, is_active
            FROM polls
            WHERE chat_id=? AND month_key=?
            ORDER BY created_at DESC
            LIMIT 1
        """, (chat_id, month_key)).fetchone()


def months_text_and_kb(chat_id: int, page: int) -> Tuple[str, InlineKeyboardMarkup]:
    keys = months_union(chat_id)
    if not keys:
        return ("В этом чате ещё нет месяцев.", InlineKeyboardMarkup([]))

    total = len(keys)
    if page < 0:
        page = 0
    max_page = max(0, (total - 1) // MONTHS_PAGE_SIZE)
    if page > max_page:
        page = max_page

    start = page * MONTHS_PAGE_SIZE
    slice_keys = keys[start:start + MONTHS_PAGE_SIZE]

    buttons: List[List[InlineKeyboardButton]] = []
    for mk in slice_keys:
        # Abonement entry
        abon = latest_abon_for_month(chat_id, mk)
        if abon:
            poll_id, _, is_active = abon
            paid, unpaid = count_paid_unpaid_abon(poll_id)
            status = "🟢" if int(is_active) == 1 else "⚪️"
            buttons.append([InlineKeyboardButton(
                f"{status} [A] {month_key_to_label(mk)} — ✅{paid} / ❗{unpaid}",
                callback_data=f"aview|{poll_id}|{MODE_ALL}|0"
            )])

        # Games entry
        if games_month_has_any(chat_id, mk):
            paid_g, unpaid_g = games_count_paid_unpaid(chat_id, mk)
            total_pln = games_get_month_total(chat_id, mk)
            sum_txt = f"{total_pln}PLN" if total_pln is not None else "—"
            buttons.append([InlineKeyboardButton(
                f"🎮 [G] {month_key_to_label(mk)} — ✅{paid_g} / ❗{unpaid_g} | {sum_txt}",
                callback_data=f"gview|{mk}|{MODE_ALL}|0"
            )])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"months|{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Стр. {page+1}/{max_page+1}", callback_data="noop"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"months|{page+1}"))
    buttons.append(nav_row)

    return "📆 Месяцы (Абонемент и Разовые игры):", InlineKeyboardMarkup(buttons)


# ---------------- Разовые игры: расчёт суммы за месяц ----------------

def compute_games_total(chat_id: int, month_key: str, total_pln: int) -> Tuple[int, int]:
    """
    Вариант A: (макс 1 игра в день, но устойчиво и к >1)
    1) делим сумму поровну по дням (в грошах честно)
    2) сумму дня делим на участников дня (ceil вверх до PLN)
    Возвращает (кол-во дней D, кол-во пользователей с долгом)
    """
    with db() as conn:
        rows = conn.execute("""
            SELECT gp.game_date,
                   (
                     SELECT COUNT(DISTINCT gv.user_id)
                     FROM game_votes gv
                     WHERE gv.poll_id=gp.poll_id AND gv.choice=0
                   ) AS yes_count
            FROM game_polls gp
            WHERE gp.chat_id=? AND gp.month_key=?
            ORDER BY gp.game_date ASC
        """, (chat_id, month_key)).fetchall()

    yes_by_day: Dict[str, int] = {}
    for d, yes_count in rows:
        yes_by_day[d] = max(yes_by_day.get(d, 0), int(yes_count))

    active_days = [d for d in yes_by_day if yes_by_day[d] > 0]
    if not active_days:
        return (0, 0)

    D = len(active_days)

    total_grosz = total_pln * 100
    base = total_grosz // D
    rem = total_grosz % D

    active_days_sorted = sorted(active_days)
    day_share: Dict[str, int] = {}
    for idx, d in enumerate(active_days_sorted):
        day_share[d] = int(base + (1 if idx < rem else 0))

    user_amounts: Dict[int, int] = {}
    computed_at = now_iso()

    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO games_month_totals(chat_id, month_key, total_amount_pln, computed_at)
            VALUES(?,?,?,?)
        """, (chat_id, month_key, total_pln, computed_at))

        conn.execute("DELETE FROM games_day_charges WHERE chat_id=? AND month_key=?", (chat_id, month_key))
        conn.execute("DELETE FROM games_user_charges WHERE chat_id=? AND month_key=?", (chat_id, month_key))

        for d in active_days_sorted:
            yes_count = yes_by_day[d]
            share_g = day_share[d]
            denom = yes_count * 100
            per_person_pln = (share_g + denom - 1) // denom  # ceil in PLN

            conn.execute("""
                INSERT OR REPLACE INTO games_day_charges(chat_id, month_key, game_date, day_share_grosz, yes_count, per_person_pln, computed_at)
                VALUES(?,?,?,?,?,?,?)
            """, (chat_id, month_key, d, share_g, yes_count, int(per_person_pln), computed_at))

            voters = conn.execute("""
                SELECT DISTINCT gv.user_id
                FROM game_polls gp
                JOIN game_votes gv ON gv.poll_id=gp.poll_id
                WHERE gp.chat_id=? AND gp.month_key=? AND gp.game_date=? AND gv.choice=0
            """, (chat_id, month_key, d)).fetchall()

            for (uid,) in voters:
                uid = int(uid)
                user_amounts[uid] = user_amounts.get(uid, 0) + int(per_person_pln)

        for uid, amount in user_amounts.items():
            conn.execute("""
                INSERT OR REPLACE INTO games_user_charges(chat_id, month_key, user_id, amount_pln, computed_at)
                VALUES(?,?,?,?,?)
            """, (chat_id, month_key, uid, amount, computed_at))

    return (D, len(user_amounts))


# ---------------- Commands ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()
    await update.message.reply_text(
        "Кнопки снизу 👇\n"
        "Если что — /cancel отменяет ввод.",
        reply_markup=kb
    )


async def help_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()
    await update.message.reply_text(
        "Основное:\n"
        "• Абонемент: /poll <сумма> <год> <месяц> и /close\n"
        "• Разовая игра: /game <название> и /close_game\n"
        "• Сумма разовых игр: /games_total <сумма> <год> <месяц>\n"
        "• /months — меню месяцев (админ)\n"
        "• /debt — твои долги\n\n"
        "Но лучше пользуйся кнопками снизу 🙂",
        reply_markup=kb
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["state"] = S_NONE
    context.user_data.pop("tmp", None)
    if update.message:
        kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()
        await update.message.reply_text("Ок, отменил.", reply_markup=kb)


async def poll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    try:
        amount, year, month_num, month_name = parse_poll_args(context.args)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return

    title = f"Абонемент {year} {month_name}"
    month_key = make_month_key(year, month_num)
    currency = "PLN"

    poll_message = await update.message.reply_poll(
        question=title,
        options=["✅", "❌"],
        is_anonymous=False,
        allows_multiple_answers=False,
    )
    poll = poll_message.poll

    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO polls(
                poll_id, chat_id, poll_message_id, title, month_key, year, month_num, month_name,
                currency, total_amount, yes_votes, final_per_person, created_at, closed_at, is_active
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
        """, (
            poll.id, update.effective_chat.id, poll_message.message_id,
            title, month_key, year, month_num, month_name,
            currency, amount, poll.options[0].voter_count,
            None, now_iso(), None
        ))

    await update.message.reply_text(f"✅ Создал абонемент: {title}\n💰 Сумма: {amount} {currency}")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только админ может закрывать абонемент.")
        return

    row = get_latest_active_abon(update.effective_chat.id)
    if not row:
        await update.message.reply_text("Нет активного абонемента.")
        return

    poll_id, chat_id, msg_id, title, _, currency, total_amount, yes_votes, _, _ = row

    try:
        await context.bot.stop_poll(chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        await update.message.reply_text("⚠️ Не смог остановить опрос (проверь права админа у бота).")

    final = ceil_div(int(total_amount), int(yes_votes))
    with db() as conn:
        conn.execute("""
            UPDATE polls
            SET is_active=0, final_per_person=?, closed_at=?
            WHERE poll_id=?
        """, (final, now_iso(), poll_id))

    if final is None:
        await update.message.reply_text(f"✅ Закрыл: {title}\nНикто не ✅ — сумму на человека не посчитать.")
    else:
        await update.message.reply_text(f"✅ Закрыл: {title}\nК оплате: {final} {currency} с человека (вверх).")


async def game_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только админ может создавать разовые игры.")
        return

    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Формат: /game <название>")
        return

    today = date.today().strftime("%Y-%m-%d")
    month_key = date.today().strftime("%Y-%m")
    title = f"Игра: {name} ({today})"

    # защита: максимум 1 игра в день
    with db() as conn:
        exists = conn.execute("""
            SELECT 1 FROM game_polls WHERE chat_id=? AND game_date=? LIMIT 1
        """, (update.effective_chat.id, today)).fetchone()
    if exists:
        await update.message.reply_text("⚠️ На сегодня уже есть разовая игра. (По условию максимум 1 в день)")
        return

    poll_message = await update.message.reply_poll(
        question=title,
        options=["✅", "❌"],
        is_anonymous=False,
        allows_multiple_answers=False,
    )
    poll = poll_message.poll

    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO game_polls(
                chat_id, poll_id, poll_message_id, title, game_date, month_key, currency,
                yes_votes, created_at, closed_at, is_active
            ) VALUES(?,?,?,?,?,?,?,?,?,?,1)
        """, (
            update.effective_chat.id, poll.id, poll_message.message_id,
            title, today, month_key, "PLN",
            poll.options[0].voter_count, now_iso(), None
        ))

    await update.message.reply_text(f"🎮 Создал разовую игру: {title}")


async def close_game_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только админ может закрывать разовые игры.")
        return

    row = get_latest_active_game(update.effective_chat.id)
    if not row:
        await update.message.reply_text("Нет активной разовой игры.")
        return

    chat_id, poll_id, msg_id, title, _, _, yes_votes = row

    try:
        await context.bot.stop_poll(chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        await update.message.reply_text("⚠️ Не смог остановить игру (проверь права админа у бота).")

    with db() as conn:
        conn.execute("UPDATE game_polls SET is_active=0, closed_at=? WHERE poll_id=?", (now_iso(), poll_id))

    await update.message.reply_text(f"✅ Закрыл игру: {title}\n✅ За: {yes_votes}")


async def games_total_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только админ может вводить сумму разовых игр.")
        return

    try:
        month_key, total_pln = parse_month_and_amount_args(context.args)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return

    D, users_count = compute_games_total(update.effective_chat.id, month_key, total_pln)
    if D == 0:
        await update.message.reply_text("В этом месяце нет разовых игр с участниками ✅ — нечего делить.")
        return

    await update.message.reply_text(
        f"✅ Зафиксировал разовые игры за {month_key_to_label(month_key)}\n"
        f"💰 Сумма: {total_pln} PLN\n"
        f"📅 Дней с участниками ✅: {D}\n"
        f"👥 Участников с долгом: {users_count}\n"
        f"Смотри /months → [G]"
    )


async def months_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только админ может смотреть /months.")
        return

    text, kb = months_text_and_kb(update.effective_chat.id, page=0)
    await update.message.reply_text(text, reply_markup=kb)


async def debt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    with db() as conn:
        abon_rows = conn.execute("""
            SELECT p.title, p.currency, p.total_amount, p.yes_votes, p.final_per_person, p.is_active
            FROM votes v
            JOIN polls p ON p.poll_id=v.poll_id
            LEFT JOIN payments pay ON pay.poll_id=v.poll_id AND pay.user_id=v.user_id
            WHERE p.chat_id=? AND v.user_id=? AND v.choice=0 AND pay.user_id IS NULL
            ORDER BY p.month_key DESC, p.created_at DESC
        """, (chat_id, user_id)).fetchall()

        game_rows = conn.execute("""
            SELECT c.month_key, c.amount_pln
            FROM games_user_charges c
            LEFT JOIN games_payments p
              ON p.chat_id=c.chat_id AND p.month_key=c.month_key AND p.user_id=c.user_id
            WHERE c.chat_id=? AND c.user_id=? AND c.amount_pln>0 AND p.user_id IS NULL
            ORDER BY c.month_key DESC
        """, (chat_id, user_id)).fetchall()

    if not abon_rows and not game_rows:
        kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()
        await update.message.reply_text("🎉 У тебя нет задолженностей.", reply_markup=kb)
        return

    lines: List[str] = []
    total_sum = 0

    if abon_rows:
        lines.append("🧾 Абонемент:")
        for title, currency, total_amount, yes_votes, final_pp, is_active in abon_rows:
            if int(is_active) == 0 and final_pp is not None:
                per = int(final_pp)
            else:
                per = ceil_div(int(total_amount), int(yes_votes)) or 0
            lines.append(f"• {title}: {per} {currency}")
            total_sum += int(per)

    if game_rows:
        lines.append("\n🎮 Разовые игры:")
        for mk, amount_pln in game_rows:
            lines.append(f"• {month_key_to_label(mk)}: {int(amount_pln)} PLN")
            total_sum += int(amount_pln)

    lines.append(f"\nИтого: {total_sum} PLN")
    kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()
    await update.message.reply_text("\n".join(lines), reply_markup=kb)


# ---------------- Callbacks ----------------

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    q = update.callback_query

    if q.data == "noop":
        await q.answer()
        return

    if not await is_admin(update, context):
        await q.answer("Только для админов", show_alert=True)
        return

    parts = (q.data or "").split("|")
    action = parts[0] if parts else ""

    # months|page
    if action == "months" and len(parts) == 2:
        page = int(parts[1])
        text, kb = months_text_and_kb(update.effective_chat.id, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer()
        return

    # aview|poll_id|mode|page
    if action == "aview" and len(parts) == 4:
        poll_id = parts[1]
        mode = parts[2] if parts[2] in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL
        page = int(parts[3])
        poll_row = get_abon_by_id(poll_id)
        if not poll_row:
            await q.answer("Опрос не найден", show_alert=True)
            return
        text, kb = abon_people_text_and_kb(poll_row, mode, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer()
        return

    # apay|poll_id|user_id|mode|page
    if action == "apay" and len(parts) == 5:
        poll_id = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        abon_set_paid(poll_id, user_id)
        poll_row = get_abon_by_id(poll_id)
        if not poll_row:
            await q.answer("Опрос не найден", show_alert=True)
            return
        text, kb = abon_people_text_and_kb(poll_row, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer("Отмечено ✅")
        return

    # aunpay|poll_id|user_id|mode|page
    if action == "aunpay" and len(parts) == 5:
        poll_id = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        abon_unset_paid(poll_id, user_id)
        poll_row = get_abon_by_id(poll_id)
        if not poll_row:
            await q.answer("Опрос не найден", show_alert=True)
            return
        text, kb = abon_people_text_and_kb(poll_row, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer("Отмена ↩️")
        return

    # gview|month_key|mode|page
    if action == "gview" and len(parts) == 4:
        month_key = parts[1]
        mode = parts[2] if parts[2] in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL
        page = int(parts[3])
        text, kb = games_month_text_and_kb(update.effective_chat.id, month_key, mode, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer()
        return

    # gpay|month_key|user_id|mode|page
    if action == "gpay" and len(parts) == 5:
        month_key = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        games_set_paid(update.effective_chat.id, month_key, user_id)
        text, kb = games_month_text_and_kb(update.effective_chat.id, month_key, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer("Отмечено ✅")
        return

    # gunpay|month_key|user_id|mode|page
    if action == "gunpay" and len(parts) == 5:
        month_key = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        games_unset_paid(update.effective_chat.id, month_key, user_id)
        text, kb = games_month_text_and_kb(update.effective_chat.id, month_key, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer("Отмена ↩️")
        return

    await q.answer()


# ---------------- Poll answers ----------------

async def on_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.poll_answer:
        return

    poll_id = update.poll_answer.poll_id
    user = update.poll_answer.user
    option_ids = update.poll_answer.option_ids
    new_choice = option_ids[0] if option_ids else None  # None = cancel vote

    upsert_user(user)

    with db() as conn:
        # abon poll?
        p = conn.execute("SELECT is_active, yes_votes FROM polls WHERE poll_id=?", (poll_id,)).fetchone()
        if p:
            is_active, yes_votes = p
            if int(is_active) == 0:
                return
            prev = conn.execute("SELECT choice FROM votes WHERE poll_id=? AND user_id=?", (poll_id, user.id)).fetchone()
            prev_choice = prev[0] if prev else None

            yes_votes = int(yes_votes)
            if prev_choice == 0:
                yes_votes -= 1
            if new_choice == 0:
                yes_votes += 1
            if yes_votes < 0:
                yes_votes = 0

            conn.execute("""
                INSERT OR REPLACE INTO votes(poll_id, user_id, choice, updated_at)
                VALUES(?,?,?,?)
            """, (poll_id, user.id, new_choice, now_iso()))
            conn.execute("UPDATE polls SET yes_votes=? WHERE poll_id=?", (yes_votes, poll_id))
            return

        # game poll?
        g = conn.execute("SELECT is_active, yes_votes FROM game_polls WHERE poll_id=?", (poll_id,)).fetchone()
        if g:
            is_active, yes_votes = g
            if int(is_active) == 0:
                return
            prev = conn.execute("SELECT choice FROM game_votes WHERE poll_id=? AND user_id=?", (poll_id, user.id)).fetchone()
            prev_choice = prev[0] if prev else None

            yes_votes = int(yes_votes)
            if prev_choice == 0:
                yes_votes -= 1
            if new_choice == 0:
                yes_votes += 1
            if yes_votes < 0:
                yes_votes = 0

            conn.execute("""
                INSERT OR REPLACE INTO game_votes(poll_id, user_id, choice, updated_at)
                VALUES(?,?,?,?)
            """, (poll_id, user.id, new_choice, now_iso()))
            conn.execute("UPDATE game_polls SET yes_votes=? WHERE poll_id=?", (yes_votes, poll_id))
            return


# ---------------- Button router (Reply keyboard) ----------------

async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    text = (update.message.text or "").strip()
    kb = admin_keyboard() if await is_admin(update, context) else user_keyboard()

    # state machine for inputs
    state = context.user_data.get("state", S_NONE)
    tmp = context.user_data.get("tmp", {})

    if state != S_NONE:
        if text.startswith("/"):
            # если пользователь написал команду во время ввода — лучше пусть команда отработает отдельно
            return

        if state == S_GAME:
            # ожидаем название игры
            name = text.strip()

            # очищаем состояние
            context.user_data["state"] = S_NONE
            context.user_data.pop("tmp", None)

            if not await is_admin(update, context):
                await update.message.reply_text("Только админ.", reply_markup=kb)
                return

            if not name:
                await update.message.reply_text("Название пустое. Попробуй ещё раз.", reply_markup=kb)
                return

            today = date.today().strftime("%Y-%m-%d")
            month_key = date.today().strftime("%Y-%m")
            title = f"Игра: {name} ({today})"

            # максимум 1 игра в день
            with db() as conn:
                exists = conn.execute(
                    "SELECT 1 FROM game_polls WHERE chat_id=? AND game_date=? LIMIT 1",
                    (update.effective_chat.id, today)
                ).fetchone()
            if exists:
                await update.message.reply_text("⚠️ На сегодня уже есть разовая игра.", reply_markup=kb)
                return

            poll_message = await update.message.reply_poll(
                question=title,
                options=["✅", "❌"],
                is_anonymous=False,
                allows_multiple_answers=False,
            )
            poll = poll_message.poll

            with db() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO game_polls(
                        chat_id, poll_id, poll_message_id, title, game_date, month_key, currency,
                        yes_votes, created_at, closed_at, is_active
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,1)
                """, (
                    update.effective_chat.id,
                    poll.id,
                    poll_message.message_id,
                    title,
                    today,
                    month_key,
                    "PLN",
                    poll.options[0].voter_count,
                    now_iso(),
                    None
                ))

            await update.message.reply_text(f"🎮 Создал разовую игру: {title}", reply_markup=kb)
            return

        if state == S_ABON:
            step = int(tmp.get("step", 1))
            if step == 1:
                digits = "".join(ch for ch in text if ch.isdigit())
                if not digits:
                    await update.message.reply_text("Введи сумму числом, например 1400", reply_markup=kb)
                    return
                tmp["amount"] = int(digits)
                tmp["step"] = 2
                context.user_data["tmp"] = tmp
                await update.message.reply_text("Теперь введи год (например 2026)", reply_markup=kb)
                return
            if step == 2:
                if not text.isdigit():
                    await update.message.reply_text("Год должен быть числом, например 2026", reply_markup=kb)
                    return
                tmp["year"] = int(text)
                tmp["step"] = 3
                context.user_data["tmp"] = tmp
                await update.message.reply_text("Теперь введи месяц словом (например март)", reply_markup=kb)
                return
            if step == 3:
                month_name = text.strip().lower()
                if month_name not in RU_MONTHS:
                    await update.message.reply_text("Месяц должен быть словом: январь, февраль, март...", reply_markup=kb)
                    return
                amount = int(tmp["amount"])
                year = int(tmp["year"])
                # clear state
                context.user_data["state"] = S_NONE
                context.user_data.pop("tmp", None)
                # create abon poll
                title = f"Абонемент {year} {month_name}"
                month_key = make_month_key(year, RU_MONTHS[month_name])
                currency = "PLN"
                poll_message = await update.message.reply_poll(
                    question=title,
                    options=["✅", "❌"],
                    is_anonymous=False,
                    allows_multiple_answers=False,
                )
                poll = poll_message.poll
                with db() as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO polls(
                            poll_id, chat_id, poll_message_id, title, month_key, year, month_num, month_name,
                            currency, total_amount, yes_votes, final_per_person, created_at, closed_at, is_active
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                    """, (
                        poll.id, update.effective_chat.id, poll_message.message_id,
                        title, month_key, year, RU_MONTHS[month_name], month_name,
                        currency, amount, poll.options[0].voter_count,
                        None, now_iso(), None
                    ))
                await update.message.reply_text(f"✅ Создал абонемент: {title}\n💰 Сумма: {amount} {currency}", reply_markup=kb)
                return

        if state == S_GAMES_TOTAL:
            step = int(tmp.get("step", 1))
            if step == 1:
                digits = "".join(ch for ch in text if ch.isdigit())
                if not digits:
                    await update.message.reply_text("Введи сумму числом, например 900", reply_markup=kb)
                    return
                tmp["amount"] = int(digits)
                tmp["step"] = 2
                context.user_data["tmp"] = tmp
                await update.message.reply_text("Теперь введи год (например 2026)", reply_markup=kb)
                return
            if step == 2:
                if not text.isdigit():
                    await update.message.reply_text("Год должен быть числом, например 2026", reply_markup=kb)
                    return
                tmp["year"] = int(text)
                tmp["step"] = 3
                context.user_data["tmp"] = tmp
                await update.message.reply_text("Теперь введи месяц словом (например март)", reply_markup=kb)
                return
            if step == 3:
                month_name = text.strip().lower()
                if month_name not in RU_MONTHS:
                    await update.message.reply_text("Месяц должен быть словом: январь, февраль, март...", reply_markup=kb)
                    return
                amount = int(tmp["amount"])
                year = int(tmp["year"])
                month_key = make_month_key(year, RU_MONTHS[month_name])
                # clear state
                context.user_data["state"] = S_NONE
                context.user_data.pop("tmp", None)

                if not await is_admin(update, context):
                    await update.message.reply_text("Только админ.", reply_markup=kb)
                    return

                D, users_count = compute_games_total(update.effective_chat.id, month_key, amount)
                if D == 0:
                    await update.message.reply_text("В этом месяце нет разовых игр с участниками ✅ — нечего делить.", reply_markup=kb)
                    return
                await update.message.reply_text(
                    f"✅ Зафиксировал разовые игры за {month_key_to_label(month_key)}\n"
                    f"💰 Сумма: {amount} PLN\n"
                    f"📅 Дней с участниками ✅: {D}\n"
                    f"👥 Участников с долгом: {users_count}\n"
                    f"Теперь /months → [G]",
                    reply_markup=kb
                )
                return

    # no active state: route buttons
    if text == "ℹ️ Помощь":
        await help_msg(update, context)
        return

    if text == "📌 Мои долги":
        await debt_cmd(update, context)
        return

    if text == "📆 Месяцы":
        await months_cmd(update, context)
        return

    # Admin buttons
    if text == "🧾 Абонемент":
        if not await is_admin(update, context):
            await update.message.reply_text("Только админ.", reply_markup=kb)
            return
        context.user_data["state"] = S_ABON
        context.user_data["tmp"] = {"step": 1}
        await update.message.reply_text("Введи сумму абонемента (например 1400). Отмена: /cancel", reply_markup=kb)
        return

    if text == "✅ Закрыть абонемент":
        await close_cmd(update, context)
        return

    if text == "🎮 Игра":
        if not await is_admin(update, context):
            await update.message.reply_text("Только админ.", reply_markup=kb)
            return
        context.user_data["state"] = S_GAME
        context.user_data["tmp"] = {}
        await update.message.reply_text("Введи название игры (создам опрос на сегодня). Отмена: /cancel", reply_markup=kb)
        return

    if text == "✅ Закрыть игру":
        await close_game_cmd(update, context)
        return

    if text == "💰 Сумма игр за месяц":
        if not await is_admin(update, context):
            await update.message.reply_text("Только админ.", reply_markup=kb)
            return
        context.user_data["state"] = S_GAMES_TOTAL
        context.user_data["tmp"] = {"step": 1}
        await update.message.reply_text("Введи сумму разовых игр за месяц (например 900). Отмена: /cancel", reply_markup=kb)
        return


# ---------------- Main ----------------

def main() -> None:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("Укажи BOT_TOKEN в переменной окружения.")
    init_db()

    app = Application.builder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_msg))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(CommandHandler("poll", poll_cmd))
    app.add_handler(CommandHandler("close", close_cmd))

    app.add_handler(CommandHandler("game", game_cmd))
    app.add_handler(CommandHandler("close_game", close_game_cmd))
    app.add_handler(CommandHandler("games_total", games_total_cmd))

    app.add_handler(CommandHandler("months", months_cmd))
    app.add_handler(CommandHandler("debt", debt_cmd))

    # Buttons (reply keyboard) + free text router
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_router))

    # Inline callbacks (months + views + pay toggles)
    app.add_handler(CallbackQueryHandler(callbacks))

    # Poll answers
    app.add_handler(PollAnswerHandler(on_poll_answer))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()