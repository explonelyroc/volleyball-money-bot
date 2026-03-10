import os
import math
import sqlite3
import logging
import threading
from datetime import datetime, date, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Tuple, List, Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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

# ===== ADMIN ACCESS (edit this) =====
ADMIN_IDS = {
    326378779,  # <-- сюда вставь свой user_id
    434566055,  # можно добавить ещё
    # 222222222, # можно добавить ещё

}


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

MONTHS_ORDER = [
    ("январь", 1), ("февраль", 2), ("март", 3), ("апрель", 4),
    ("май", 5), ("июнь", 6), ("июль", 7), ("август", 8),
    ("сентябрь", 9), ("октябрь", 10), ("ноябрь", 11), ("декабрь", 12),
]


def year_keyboard(prefix: str) -> InlineKeyboardMarkup:
    """Кнопки выбора года: предыдущий / текущий / следующий."""
    cur = date.today().year
    years = [cur - 1, cur, cur + 1]
    buttons = [InlineKeyboardButton(str(y), callback_data=f"{prefix}year:{y}") for y in years]
    return InlineKeyboardMarkup([buttons])


def month_keyboard(prefix: str) -> InlineKeyboardMarkup:
    """Кнопки выбора месяца — 4 строки по 3."""
    rows = []
    for i in range(0, 12, 3):
        row = [
            InlineKeyboardButton(
                name.capitalize(),
                callback_data=f"{prefix}month:{num}"
            )
            for name, num in MONTHS_ORDER[i:i+3]
        ]
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

# ↓↓↓ ВСТАВИТЬ СЮДА ↓↓↓
async def remember_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    if chat.type not in ("group", "supergroup"):
        return

    with db() as conn:
        conn.execute("""
            INSERT INTO known_chats(chat_id, title, chat_type, last_seen_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
              title=excluded.title,
              chat_type=excluded.chat_type,
              last_seen_at=excluded.last_seen_at
        """, (chat.id, chat.title or str(chat.id), chat.type, now_iso()))

async def remember_chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await remember_chat(update, context)
# ↑↑↑ КОНЕЦ ВСТАВКИ ↑↑↑

def make_month_key(year: int, month_num: int) -> str:
    return f"{year:04d}-{month_num:02d}"

def is_admin_user_id(user_id: int) -> bool:
    return user_id in ADMIN_IDS


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
                -- ===== Управление (личка админа) =====
        CREATE TABLE IF NOT EXISTS known_chats (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            chat_type TEXT,
            last_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS admin_state (
            admin_id INTEGER PRIMARY KEY,
            active_chat_id INTEGER,
            updated_at TEXT NOT NULL
        );
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

# ⬇⬇⬇ ВСТАВИТЬ ЗДЕСЬ ⬇⬇⬇

def get_active_chat_id(admin_id: int):
    with db() as conn:
        row = conn.execute(
            "SELECT active_chat_id FROM admin_state WHERE admin_id=?",
            (admin_id,),
        ).fetchone()
        if not row:
            return None
        return row[0]


def set_active_chat_id(admin_id: int, chat_id: int) -> None:
    with db() as conn:
        conn.execute("""
            INSERT INTO admin_state(admin_id, active_chat_id, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(admin_id) DO UPDATE SET
              active_chat_id=excluded.active_chat_id,
              updated_at=excluded.updated_at
        """, (admin_id, chat_id, now_iso()))

def resolve_target_chat_id(update: Update) -> int | None:
    """
    Возвращает chat_id группы, с которой админ сейчас работает.
    Если команда в группе — это сама группа.
    Если команда в личке — берём выбранный active_chat_id из admin_state.
    """
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return None

    if chat.type in ("group", "supergroup"):
        return chat.id

    # личка
    return get_active_chat_id(user.id)


# ⬆⬆⬆ ДО ЭТОЙ СТРОКИ ⬆⬆⬆

def display_name_expr() -> str:
    return """
    COALESCE(
      NULLIF(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), ''),
      COALESCE(NULLIF(u.username,''), 'user')
    ) AS display_name
    """


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    # Главная проверка: whitelist
    return update.effective_user and (update.effective_user.id in ADMIN_IDS)


def admin_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("🧾 Абонемент"), KeyboardButton("✅ Закрыть абонемент")],
        [KeyboardButton("🎮 Вторник"), KeyboardButton("✅ Закрыть Вторник")],
        [KeyboardButton("💰 Сумма игр за Вторники"), KeyboardButton("📆 Список оплат")],
        [KeyboardButton("📣 Должники"), KeyboardButton("📌 Мои долги")],
        [KeyboardButton("ℹ️ Помощь")],
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

    return "📆 Список оплат (Абонемент и Разовые игры):", InlineKeyboardMarkup(buttons)


# ---------------- Разовые игры: расчёт суммы за месяц ----------------

def compute_games_total(chat_id: int, month_key: str, total_pln: int) -> Tuple[int, int]:
    """
    Считает распределение суммы за месяц по разовым играм.

    Логика (поддерживает несколько игр в один день):
      1. Считаем общее кол-во игро-слотов за месяц: для каждой игры берём
         UNIQUE уникальных участников (choice=0). Суммируем все yes_count по
         всем играм — это N_total (человеко-посещений).
      2. Стоимость одного человеко-посещения = total_pln / N_total (в грошах, ceil).
      3. Каждому пользователю начисляем: кол-во игр где он был × стоимость.

    Для games_day_charges записываем агрегат по дню (сумма всех игр дня).
    Возвращает (кол-во игр с участниками, кол-во пользователей с долгом).
    """
    with db() as conn:
        # Получаем все игры месяца с их голосами
        game_rows = conn.execute("""
            SELECT gp.game_id, gp.poll_id, gp.game_date
            FROM game_polls gp
            WHERE gp.chat_id=? AND gp.month_key=?
            ORDER BY gp.game_date ASC, gp.game_id ASC
        """, (chat_id, month_key)).fetchall()

        if not game_rows:
            return (0, 0)

        # Для каждой игры — список участников (choice=0)
        game_voters: Dict[str, List[int]] = {}  # poll_id -> [user_id, ...]
        for _, poll_id, _ in game_rows:
            voters = conn.execute("""
                SELECT DISTINCT user_id FROM game_votes
                WHERE poll_id=? AND choice=0
            """, (poll_id,)).fetchall()
            game_voters[poll_id] = [r[0] for r in voters]

    # Только игры с хотя бы 1 участником
    active_games = [(gid, pid, gdate) for gid, pid, gdate in game_rows if game_voters.get(pid)]
    if not active_games:
        return (0, 0)

    # Общее кол-во человеко-посещений
    n_total = sum(len(game_voters[pid]) for _, pid, _ in active_games)
    if n_total == 0:
        return (0, 0)

    # Стоимость одного посещения в грошах (ceil)
    total_grosz = total_pln * 100
    cost_per_visit_grosz = math.ceil(total_grosz / n_total)

    # Начисления пользователям
    user_amounts: Dict[int, int] = {}
    for _, pid, _ in active_games:
        for uid in game_voters[pid]:
            user_amounts[uid] = user_amounts.get(uid, 0) + cost_per_visit_grosz

    # Переводим грошы в PLN (ceil до целого PLN)
    user_amounts_pln = {uid: math.ceil(grosz / 100) for uid, grosz in user_amounts.items()}

    # Агрегат по дням для games_day_charges
    day_yes_count: Dict[str, int] = {}
    day_share_grosz: Dict[str, int] = {}
    for _, pid, gdate in active_games:
        cnt = len(game_voters[pid])
        day_yes_count[gdate] = day_yes_count.get(gdate, 0) + cnt
        day_share_grosz[gdate] = day_share_grosz.get(gdate, 0) + cnt * cost_per_visit_grosz

    computed_at = now_iso()

    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO games_month_totals(chat_id, month_key, total_amount_pln, computed_at)
            VALUES(?,?,?,?)
        """, (chat_id, month_key, total_pln, computed_at))

        conn.execute("DELETE FROM games_day_charges WHERE chat_id=? AND month_key=?", (chat_id, month_key))
        conn.execute("DELETE FROM games_user_charges WHERE chat_id=? AND month_key=?", (chat_id, month_key))

        for gdate, yes_cnt in day_yes_count.items():
            share_g = day_share_grosz[gdate]
            per_person_pln = math.ceil(share_g / (yes_cnt * 100)) if yes_cnt else 0
            conn.execute("""
                INSERT OR REPLACE INTO games_day_charges(
                    chat_id, month_key, game_date, day_share_grosz, yes_count, per_person_pln, computed_at)
                VALUES(?,?,?,?,?,?,?)
            """, (chat_id, month_key, gdate, share_g, yes_cnt, per_person_pln, computed_at))

        for uid, amount_pln in user_amounts_pln.items():
            conn.execute("""
                INSERT OR REPLACE INTO games_user_charges(chat_id, month_key, user_id, amount_pln, computed_at)
                VALUES(?,?,?,?,?)
            """, (chat_id, month_key, uid, amount_pln, computed_at))

    return (len(active_games), len(user_amounts_pln))


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

async def myid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(f"Ваш Telegram user_id: {update.effective_user.id}")


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

async def manage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return

    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("⛔ Нет доступа.")
        return

    # если написали в группе — отправляем в личку
    if update.effective_chat.type in ("group", "supergroup"):
        bot_username = context.bot.username
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("⚙️ Открыть панель в личке", url=f"https://t.me/{bot_username}?start=manage")
        ]])
        await update.message.reply_text("Управление ботом — в личных сообщениях ✅", reply_markup=kb)
        return

    # личка
    admin_id = update.effective_user.id
    active = get_active_chat_id(admin_id)
    active_txt = str(active) if active else "не выбран"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Выбрать чат", callback_data="adm:choose_chat")],
    ])
    await update.message.reply_text(f"⚙️ Панель управления\nТекущий чат: {active_txt}", reply_markup=kb)


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

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    poll_message = await context.bot.send_poll(
        chat_id=target_chat_id,
        question=title,
        options=["беру ✅", "не беру❌"],
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
            poll.id, target_chat_id, poll_message.message_id,
            title, month_key, year, month_num, month_name,
            currency, amount, poll.options[0].voter_count,
            None, now_iso(), None
        ))

    await update.message.reply_text(f"✅ Создал абонемент: {title}\n💰 Сумма: {amount} {currency}")


async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ может закрывать абонемент.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    row = get_latest_active_abon(target_chat_id)
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
        result_text = f"✅ Закрыл: {title}\nНикто не ✅ — сумму на человека не посчитать."
    else:
        result_text = f"✅ Закрыл: {title}\nК оплате: {final} {currency} с человека (вверх)."

    await update.message.reply_text(
        result_text + "\n\nОтправить итог в чат?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📤 Да, отправить", callback_data=f"sendchat:abon_close:{poll_id}"),
            InlineKeyboardButton("Нет", callback_data="noop"),
        ]])
    )


async def game_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ может создавать разовые игры.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Формат: /game <название>")
        return

    today = date.today().strftime("%Y-%m-%d")
    month_key = date.today().strftime("%Y-%m")
    title = f"Игра: {name} ({today})"

    # защита: максимум 1 игра в день
    #with db() as conn:
    #    exists = conn.execute("""
    #        SELECT 1 FROM game_polls WHERE chat_id=? AND game_date=? LIMIT 1
    #    """, (target_chat_id, today)).fetchone()
    #if exists:
    #    await update.message.reply_text("⚠️ На сегодня уже есть разовая игра. (По условию максимум 1 в день)")
    #    return

    poll_message = await context.bot.send_poll(
        chat_id=target_chat_id,
        question=title,
        options=["иду ✅", "не иду❌"],
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
            target_chat_id, poll.id, poll_message.message_id,
            title, today, month_key, "PLN",
            poll.options[0].voter_count, now_iso(), None
        ))

    await update.message.reply_text(f"🎮 Создал разовую игру: {title}")

def gsheet_service():
    creds_path = os.environ.get("GOOGLE_CREDS")
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_CREDS не задан или файл не найден")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def sheet_title_safe(name: str) -> str:
    bad = ['\\', '/', '?', '*', '[', ']']
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.strip() or "Sheet"
    return name[:100]

def upsert_sheet(svc, spreadsheet_id: str, title: str):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if title in existing:
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]}
    ).execute()

def write_values(svc, spreadsheet_id: str, sheet_title: str, values: list[list]):
    # очистим (широко)
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A:Z",
        body={}
    ).execute()
    # запишем
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_title}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

def find_table(conn, candidates: list[str]) -> str | None:
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()}
    for c in candidates:
        if c in tables:
            return c
    return None

def cols_of(conn, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]

def has_cols(cols: list[str], needed: list[str]) -> bool:
    s = set(cols)
    return all(x in s for x in needed)

def pick_first(cols: list[str], options: list[str]) -> str | None:
    for o in options:
        if o in cols:
            return o
    return None

async def export_pretty_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только для админов.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    spreadsheet_id = os.environ.get("GSHEET_ID")
    if not spreadsheet_id:
        await update.message.reply_text("Не задан GSHEET_ID в переменных окружения.")
        return

    try:
        svc = gsheet_service()
    except Exception as e:
        await update.message.reply_text(f"Ошибка Google creds: {e}")
        return

    chat_id = target_chat_id

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row

        # ... ВЕСЬ твой код экспорта здесь ...

        # --- 1) Найдём таблицы ---
        polls_t = find_table(conn, ["polls", "abon_polls", "subscriptions", "subscription_polls"])
        games_t = find_table(conn, ["game_polls", "games", "one_time_games"])
        pays_t  = find_table(conn, ["payments", "abon_payments", "paid_marks", "pay_status"])

        # --- 2) Выгрузим сырые таблицы (на всякий случай) ---
        raw_title = "RAW_tables"
        upsert_sheet(svc, spreadsheet_id, raw_title)
        raw_values = [["table", "rows", "columns"]]
        tables = [r[0] for r in conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """).fetchall()]
        for t in tables:
            c = cols_of(conn, t)
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            raw_values.append([t, n, ", ".join(c)])
        write_values(svc, spreadsheet_id, raw_title, raw_values)

        # --- 3) “Абонементы” (месяц → сумма/на человека/список “за”/кто оплатил/долги) ---
        # --- 3) Абонементы (точно под твою БД) ---
        abon_title = "Абонементы"
        upsert_sheet(svc, spreadsheet_id, abon_title)

        abon_values = [[
            "month_key", "month_name", "title",
            "total_amount", "yes_votes", "final_per_person",
            "yes_names", "paid_names", "unpaid_names"
        ]]

        # читаем polls этого чата
        abon_rows = conn.execute("""
            SELECT poll_id, chat_id, title, month_key, month_name, currency,
                   total_amount, yes_votes, final_per_person
            FROM polls
            WHERE chat_id=?
            ORDER BY month_key DESC
        """, (chat_id,)).fetchall()

        # получаем голоса (choice=0 == ✅)
        votes_rows = conn.execute("""
            SELECT poll_id, user_id, choice
            FROM votes
        """).fetchall()

        # оплаты: наличие paid_at = оплатил
        pay_rows = conn.execute("""
            SELECT poll_id, user_id, paid_at
            FROM payments
        """).fetchall()

        # пользователи
        users_rows = conn.execute("""
            SELECT user_id, username, first_name, last_name
            FROM users
        """).fetchall()
        user_map = {}
        for u in users_rows:
            uid = u["user_id"]
            uname = u["username"] or ""
            fn = u["first_name"] or ""
            ln = u["last_name"] or ""
            label = uname if uname else (fn + " " + ln).strip()
            if not label:
                label = str(uid)
            user_map[uid] = label

        # индексы: poll -> set(user)
        yes_by_poll = {}
        for v in votes_rows:
            if v["choice"] == 0:
                yes_by_poll.setdefault(v["poll_id"], set()).add(v["user_id"])

        paid_by_poll = {}
        for p in pay_rows:
            if p["paid_at"]:
                paid_by_poll.setdefault(p["poll_id"], set()).add(p["user_id"])

        for pr in abon_rows:
            pid = pr["poll_id"]
            yes_users = yes_by_poll.get(pid, set())
            paid_users = paid_by_poll.get(pid, set())

            unpaid_users = [uid for uid in yes_users if uid not in paid_users]

            yes_names = [user_map.get(uid, str(uid)) for uid in sorted(list(yes_users))]
            paid_names = [user_map.get(uid, str(uid)) for uid in sorted(list(paid_users))]
            unpaid_names = [user_map.get(uid, str(uid)) for uid in sorted(list(unpaid_users))]

            abon_values.append([
                pr["month_key"],
                pr["month_name"],
                pr["title"],
                pr["total_amount"],
                pr["yes_votes"],
                pr["final_per_person"],
                ", ".join(yes_names),
                ", ".join(paid_names),
                ", ".join(unpaid_names),
            ])

        write_values(svc, spreadsheet_id, abon_title, abon_values)

        # --- 4) Долги (итого) ---
        debts_title = "Долги (итого)"
        upsert_sheet(svc, spreadsheet_id, debts_title)
        debts_values = [["who", "month_key", "kind", "amount_pln"]]

        # долги по абонементам: кто ✅ и не оплатил
        for pr in abon_rows:
            pid = pr["poll_id"]
            mk = pr["month_key"]
            per = pr["final_per_person"] or 0
            yes_users = yes_by_poll.get(pid, set())
            paid_users = paid_by_poll.get(pid, set())
            for uid in yes_users:
                if uid not in paid_users:
                    debts_values.append([user_map.get(uid, str(uid)), mk, "abon", per])

        # долги по разовым играм (если уже рассчитаны начисления)
        # games_user_charges: chat_id, month_key, user_id, amount_pln
        try:
            charges = conn.execute("""
                SELECT month_key, user_id, amount_pln
                FROM games_user_charges
                WHERE chat_id=?
            """, (chat_id,)).fetchall()
            for c in charges:
                debts_values.append([
                    user_map.get(c["user_id"], str(c["user_id"])),
                    c["month_key"],
                    "games",
                    c["amount_pln"]
                ])
        except Exception:
            pass

        write_values(svc, spreadsheet_id, debts_title, debts_values)
    finally:
        conn.close()

    await update.message.reply_text("✅ Красивый экспорт в Google Sheets готов: Абонементы / Разовые игры / Долги / RAW_tables")


async def close_game_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not update.effective_user or not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ может закрывать разовые игры.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    row = get_latest_active_game(target_chat_id)
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
    if not update.message or not update.effective_chat or not update.effective_user:
        return

    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ может вводить сумму разовых игр.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    try:
        month_key, total_pln = parse_month_and_amount_args(context.args)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return

    D, users_count = compute_games_total(target_chat_id, month_key, total_pln)
    if D == 0:
        await update.message.reply_text("В этом месяце нет разовых игр с участниками ✅ — нечего делить.")
        return

    result_text = (
        f"✅ Зафиксировал разовые игры за {month_key_to_label(month_key)}\n"
        f"💰 Сумма: {total_pln} PLN\n"
        f"🎮 Игр с участниками ✅: {D}\n"
        f"👥 Участников с долгом: {users_count}"
    )
    await update.message.reply_text(
        result_text + "\n\nОтправить итог в чат?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📤 Да, отправить", callback_data=f"sendchat:gtotal:{target_chat_id}:{month_key}"),
            InlineKeyboardButton("Нет", callback_data="noop"),
        ]])
    )


async def months_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return

    # права: список админов (ADMIN_IDS)
    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ может смотреть /months.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    text, kb = months_text_and_kb(target_chat_id, page=0)
    await update.message.reply_text(text, reply_markup=kb)


async def debt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return

    # в группе debt смотрим по этой группе
    # в личке debt смотрим по выбранной группе (/manage)
    if update.effective_chat.type in ("group", "supergroup"):
        chat_id = update.effective_chat.id
    else:
        chat_id = resolve_target_chat_id(update)
        if not chat_id:
            await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
            return

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

async def remind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только админ.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    with db() as conn:
        conn.row_factory = sqlite3.Row

        # Пользователи
        users = conn.execute("""
            SELECT user_id, username, first_name, last_name
            FROM users
        """).fetchall()

        user_map = {}
        for u in users:
            uid = u["user_id"]
            uname = u["username"] or ""
            fn = u["first_name"] or ""
            ln = u["last_name"] or ""
            label = uname if uname else (fn + " " + ln).strip()
            if not label:
                label = str(uid)
            user_map[uid] = label

        # Абонементные долги только по выбранному чату:
        # "за" (choice=0) и нет оплаты
        abon = conn.execute("""
            SELECT v.user_id, COALESCE(p.final_per_person, 0) AS amount
            FROM votes v
            JOIN polls p ON p.poll_id = v.poll_id
            LEFT JOIN payments pay ON pay.poll_id = v.poll_id AND pay.user_id = v.user_id
            WHERE p.chat_id = ? AND v.choice = 0 AND pay.user_id IS NULL
        """, (target_chat_id,)).fetchall()

        debts = {}
        for r in abon:
            debts[r["user_id"]] = debts.get(r["user_id"], 0) + int(r["amount"] or 0)

        # Разовые игры: начислено и не оплачено (по выбранному чату)
        games = conn.execute("""
            SELECT c.user_id, c.amount_pln
            FROM games_user_charges c
            LEFT JOIN games_payments p
              ON p.chat_id=c.chat_id AND p.month_key=c.month_key AND p.user_id=c.user_id
            WHERE c.chat_id=? AND c.amount_pln>0 AND p.user_id IS NULL
        """, (target_chat_id,)).fetchall()

        for r in games:
            debts[r["user_id"]] = debts.get(r["user_id"], 0) + int(r["amount_pln"] or 0)

    if not debts:
        await update.message.reply_text("🎉 Должников нет!")
        return

    text = "📢 Напоминание о задолженности:\n\n"
    for uid, total in debts.items():
        name = user_map.get(uid, str(uid))
        text += f"• {name} — {int(total)} PLN\n"

    await update.message.reply_text(text)


def build_debtors_lines(chat_id: int) -> Tuple[List[str], Dict[int, str]]:
    """
    Возвращает (lines, user_map) — готовый список строк для отправки
    и словарь uid→имя. Если должников нет — lines будет пустым.
    """
    with db() as conn:
        conn.row_factory = sqlite3.Row

        user_map: Dict[int, str] = {}
        for u in conn.execute("SELECT user_id, username, first_name, last_name FROM users").fetchall():
            uid = u["user_id"]
            name = (u["username"] or "").strip()
            if not name:
                name = ((u["first_name"] or "") + " " + (u["last_name"] or "")).strip()
            user_map[uid] = name or str(uid)

        abon_rows = conn.execute("""
            SELECT v.user_id,
                   p.title AS poll_title,
                   p.month_key,
                   p.currency,
                   p.total_amount,
                   p.yes_votes,
                   p.final_per_person,
                   p.is_active
            FROM votes v
            JOIN polls p ON p.poll_id = v.poll_id
            LEFT JOIN payments pay ON pay.poll_id = v.poll_id AND pay.user_id = v.user_id
            WHERE p.chat_id = ? AND v.choice = 0 AND pay.user_id IS NULL
            ORDER BY p.month_key DESC
        """, (chat_id,)).fetchall()

        game_rows = conn.execute("""
            SELECT c.user_id, c.month_key, c.amount_pln
            FROM games_user_charges c
            LEFT JOIN games_payments p
              ON p.chat_id = c.chat_id AND p.month_key = c.month_key AND p.user_id = c.user_id
            WHERE c.chat_id = ? AND c.amount_pln > 0 AND p.user_id IS NULL
            ORDER BY c.month_key DESC
        """, (chat_id,)).fetchall()

    user_debts: Dict[int, Dict] = {}

    def get_entry(uid: int) -> Dict:
        if uid not in user_debts:
            user_debts[uid] = {"abon": [], "games": []}
        return user_debts[uid]

    for r in abon_rows:
        uid = int(r["user_id"])
        is_active = int(r["is_active"])
        final_pp = r["final_per_person"]
        if is_active == 0 and final_pp is not None:
            amount = int(final_pp)
        else:
            amount = math.ceil(int(r["total_amount"]) / max(int(r["yes_votes"]), 1))
        label = f"{month_key_to_label(r['month_key'])} ({r['currency']})"
        get_entry(uid)["abon"].append((label, amount))

    for r in game_rows:
        uid = int(r["user_id"])
        label = month_key_to_label(r["month_key"])
        get_entry(uid)["games"].append((label, int(r["amount_pln"])))

    if not user_debts:
        return [], user_map

    def total_debt(uid: int) -> int:
        e = user_debts[uid]
        return sum(a for _, a in e["abon"]) + sum(a for _, a in e["games"])

    sorted_uids = sorted(user_debts.keys(), key=total_debt, reverse=True)

    lines: List[str] = [f"📋 Должники ({len(sorted_uids)} чел.):\n"]
    grand_total = 0

    for uid in sorted_uids:
        entry = user_debts[uid]
        name = user_map.get(uid, str(uid))
        person_total = total_debt(uid)
        grand_total += person_total

        lines.append(f"👤 {name} — итого {person_total} PLN")
        for label, amt in entry["abon"]:
            lines.append(f"   🧾 Абонемент {label}: {amt} PLN")
        for label, amt in entry["games"]:
            lines.append(f"   🎮 Игры {label}: {amt} PLN")

    lines.append(f"\n💰 Общий долг: {grand_total} PLN")
    return lines, user_map


async def debtors_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /debtors — полный список должников с разбивкой по абонементу и разовым играм.
    Только для админа.
    """
    if not update.message or not update.effective_user:
        return
    if not is_admin_user_id(update.effective_user.id):
        await update.message.reply_text("Только для админов.")
        return

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    lines, _ = build_debtors_lines(target_chat_id)

    if not lines:
        await update.message.reply_text("🎉 Должников нет!", reply_markup=admin_keyboard())
        return

    confirm_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📤 Отправить в чат", callback_data=f"sendchat:debtors:{target_chat_id}"),
        InlineKeyboardButton("Нет", callback_data="noop"),
    ]])

    full_text = "\n".join(lines)
    if len(full_text) <= 4096:
        await update.message.reply_text(full_text, reply_markup=confirm_kb)
    else:
        chunk: List[str] = []
        chunks: List[str] = []
        for line in lines:
            if sum(len(l) + 1 for l in chunk) + len(line) > 4000:
                chunks.append("\n".join(chunk))
                chunk = []
            chunk.append(line)
        if chunk:
            chunks.append("\n".join(chunk))
        for i, part in enumerate(chunks):
            await update.message.reply_text(
                part,
                reply_markup=confirm_kb if i == len(chunks) - 1 else None
            )


# ---------------- Callbacks ----------------

#вставка

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not is_admin_user_id(q.from_user.id):
        await q.edit_message_text("⛔ Нет доступа.")
        return

    admin_id = q.from_user.id
    data = q.data or ""

    if data == "adm:choose_chat":
        with db() as conn:
            chats = conn.execute(
                "SELECT chat_id, title FROM known_chats ORDER BY last_seen_at DESC"
            ).fetchall()

        buttons = []
        for chat_id, title in chats:
            # показываем только если админ состоит в этом чате
            try:
                m = await context.bot.get_chat_member(chat_id, admin_id)
                if m.status in ("left", "kicked"):
                    continue
            except Exception:
                continue

            buttons.append([InlineKeyboardButton(title, callback_data=f"adm:set_chat:{chat_id}")])

        if not buttons:
            await q.edit_message_text(
                "Не вижу общих чатов.\n"
                "Напишите в нужной группе любую команду боту (например /start), и она появится."
            )
            return

        await q.edit_message_text("Выберите чат:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if data.startswith("adm:set_chat:"):
        chat_id = int(data.split(":")[-1])
        set_active_chat_id(admin_id, chat_id)
        await q.edit_message_text(f"✅ Чат выбран: {chat_id}\nТеперь управление будет для этого чата.")
        return



#Вставка NOOP

async def noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if q:
        await q.answer()

#Вставка NOOP

async def months_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    # доступ только админам
    if not is_admin_user_id(q.from_user.id):
        await q.edit_message_text("⛔ Нет доступа.")
        return

    # ВАЖНО: чат берём из выбранного /manage, а не из лички
    target_chat_id = get_active_chat_id(q.from_user.id)
    if not target_chat_id:
        await q.edit_message_text("Сначала выбери чат: /manage → «Выбрать чат».")
        return

    # data вида "months|2"
    parts = (q.data or "").split("|")
    try:
        page = int(parts[1])
    except Exception:
        page = 0

    text, kb = months_text_and_kb(target_chat_id, page=page)
    await q.edit_message_text(text, reply_markup=kb)

#вставка

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    q = update.callback_query

    if q.data == "noop":
        await q.answer()
        return

    if not is_admin_user_id(q.from_user.id):
        await q.answer("Только для админов", show_alert=True)
        return

    target_chat_id = get_active_chat_id(q.from_user.id)
    if not target_chat_id:
        await q.answer("Сначала выбери чат через /manage", show_alert=True)
        return

    parts = (q.data or "").split("|")
    action = parts[0] if parts else ""

    # months|page
    #if action == "months" and len(parts) == 2:
    #    page = int(parts[1])
    #    text, kb = months_text_and_kb(target_chat_id, page)
    #    await q.edit_message_text(text, reply_markup=kb)
    #    await q.answer()
    #    return

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
        text, kb = games_month_text_and_kb(target_chat_id, month_key, mode, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer()
        return

    # gpay|month_key|user_id|mode|page
    if action == "gpay" and len(parts) == 5:
        month_key = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        games_set_paid(target_chat_id, month_key, user_id)
        text, kb = games_month_text_and_kb(target_chat_id, month_key, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
        await q.edit_message_text(text, reply_markup=kb)
        await q.answer("Отмечено ✅")
        return

    # gunpay|month_key|user_id|mode|page
    if action == "gunpay" and len(parts) == 5:
        month_key = parts[1]
        user_id = int(parts[2])
        mode = parts[3]
        page = int(parts[4])
        games_unset_paid(target_chat_id, month_key, user_id)
        text, kb = games_month_text_and_kb(target_chat_id, month_key, mode if mode in (MODE_ALL, MODE_UNPAID, MODE_PAID) else MODE_ALL, page)
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

#ВСТАВКА НА КНОПКИ ПО ЧАТУ

    target_chat_id = resolve_target_chat_id(update)
    if not target_chat_id:
        await update.message.reply_text("Сначала выбери чат: /manage → «Выбрать чат».", reply_markup=kb)
        return

#ВСТАВКА НА КНОПКИ ПО ЧАТУ

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
            if not name:
                await update.message.reply_text("Название пустое. Попробуй ещё раз.", reply_markup=kb)
                return

            # очищаем состояние
            context.user_data["state"] = S_NONE
            context.user_data.pop("tmp", None)

            # запускаем ту же логику, что и команда /game
            context.args = name.split()
            await game_cmd(update, context)
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
                await update.message.reply_text(
                    "Выбери год абонемента:",
                    reply_markup=year_keyboard("abon:")
                )
                return
            # шаги 2 и 3 обрабатываются в pick_year_month_callback

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
                await update.message.reply_text(
                    "Выбери год:",
                    reply_markup=year_keyboard("gtotal:")
                )
                return
            # шаги 2 и 3 обрабатываются в pick_year_month_callback

    # no active state: route buttons
    if text == "ℹ️ Помощь":
        await help_msg(update, context)
        return

    if text == "📌 Мои долги":
        await debt_cmd(update, context)
        return

    if text == "📆 Список оплат":
        await months_cmd(update, context)
        return

    if text == "📣 Должники":
        await debtors_cmd(update, context)
        return

    # Admin buttons
    if text == "🧾 Абонемент":
        if not is_admin_user_id(update.effective_user.id):
            await update.message.reply_text("⛔ Только для админов.")
            return
        context.user_data["state"] = S_ABON
        context.user_data["tmp"] = {"step": 1}
        await update.message.reply_text("Введи сумму абонемента (например 1400). Отмена: /cancel", reply_markup=kb)
        return

    if text == "✅ Закрыть абонемент":
        await close_cmd(update, context)
        return

    if text == "🎮 Вторник":
        if not is_admin_user_id(update.effective_user.id):
            await update.message.reply_text("⛔ Только для админов.")
            return
        context.user_data["state"] = S_GAME
        context.user_data["tmp"] = {}
        await update.message.reply_text("Введи Время игры (например: 21.00-22.30). Отмена: /cancel", reply_markup=kb)
        return

    if text == "✅ Закрыть Вторник":
        await close_game_cmd(update, context)
        return

    if text == "💰 Сумма игр за Вторники":
        if not is_admin_user_id(update.effective_user.id):
            await update.message.reply_text("⛔ Только для админов.")
            return
        context.user_data["state"] = S_GAMES_TOTAL
        context.user_data["tmp"] = {"step": 1}
        await update.message.reply_text("Введи сумму разовых игр за месяц (например 900). Отмена: /cancel", reply_markup=kb)
        return


# ---------------- Send-to-chat callback ----------------

async def send_to_chat_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает кнопку «📤 Отправить в чат».
    Форматы callback_data:
      sendchat:abon_close:<poll_id>         — итог закрытия абонемента
      sendchat:gtotal:<chat_id>:<month_key> — итог разовых игр за месяц
      sendchat:debtors:<chat_id>            — список всех должников
    """
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not is_admin_user_id(q.from_user.id):
        await q.answer("Только для админов", show_alert=True)
        return

    parts = (q.data or "").split(":")
    # parts[0] = "sendchat", parts[1] = action
    action = parts[1] if len(parts) > 1 else ""

    # ---- Итог закрытия абонемента ----
    if action == "abon_close":
        poll_id = parts[2]
        row = get_abon_by_id(poll_id)
        if not row:
            await q.edit_message_text("⚠️ Опрос не найден.")
            return
        _, chat_id, _, title, _, currency, total_amount, yes_votes, final_pp, is_active = row
        if int(is_active) == 0 and final_pp is not None:
            per = int(final_pp)
            msg = (
                f"📢 Абонемент закрыт!\n"
                f"📋 {title}\n"
                f"💰 К оплате: {per} {currency} с человека\n"
                f"✅ Проголосовали «за»: {yes_votes} чел."
            )
        else:
            msg = (
                f"📢 Абонемент создан!\n"
                f"📋 {title}\n"
                f"💰 Сумма: {total_amount} {currency}"
            )
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg)
            await q.edit_message_text(q.message.text.split("\n\nОтправить")[0] + "\n\n✅ Отправлено в чат.", reply_markup=None)
        except Exception as e:
            await q.edit_message_text(f"⚠️ Не смог отправить: {e}", reply_markup=None)
        return

    # ---- Итог разовых игр за месяц ----
    if action == "gtotal":
        chat_id = int(parts[2])
        month_key = parts[3]
        total_pln = games_get_month_total(chat_id, month_key)
        paid_cnt, unpaid_cnt = games_count_paid_unpaid(chat_id, month_key)
        label = month_key_to_label(month_key)
        msg = (
            f"📢 Разовые игры за {label}\n"
            f"💰 Сумма: {total_pln} PLN\n"
            f"✅ Оплатили: {paid_cnt} | ❗ Не оплатили: {unpaid_cnt}"
        )
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg)
            await q.edit_message_text(q.message.text.split("\n\nОтправить")[0] + "\n\n✅ Отправлено в чат.", reply_markup=None)
        except Exception as e:
            await q.edit_message_text(f"⚠️ Не смог отправить: {e}", reply_markup=None)
        return

    # ---- Список должников ----
    if action == "debtors":
        chat_id = int(parts[2])

        lines, _ = build_debtors_lines(chat_id)

        if not lines:
            await q.edit_message_text("🎉 Должников нет — нечего отправлять.", reply_markup=None)
            return

        try:
            if len("\n".join(lines)) <= 4096:
                await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
            else:
                chunk: List[str] = []
                for line in lines:
                    if sum(len(l) + 1 for l in chunk) + len(line) > 4000:
                        await context.bot.send_message(chat_id=chat_id, text="\n".join(chunk))
                        chunk = []
                    chunk.append(line)
                if chunk:
                    await context.bot.send_message(chat_id=chat_id, text="\n".join(chunk))
            await q.edit_message_text(
                q.message.text.split("\n\nОтправить")[0] + "\n\n✅ Отправлено в чат.",
                reply_markup=None
            )
        except Exception as e:
            await q.edit_message_text(f"⚠️ Не смог отправить: {e}", reply_markup=None)
        return


# ---------------- Year/Month picker callbacks ----------------

async def pick_year_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает выбор года и месяца через инлайн-кнопки для флоу S_ABON и S_GAMES_TOTAL.
    Форматы callback_data:
      abon:year:<year>    abon:month:<num>
      gtotal:year:<year>  gtotal:month:<num>
    """
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not is_admin_user_id(q.from_user.id):
        await q.answer("Только для админов", show_alert=True)
        return

    data = q.data or ""
    # Определяем префикс флоу
    if data.startswith("abon:"):
        flow = S_ABON
        prefix = "abon:"
    elif data.startswith("gtotal:"):
        flow = S_GAMES_TOTAL
        prefix = "gtotal:"
    else:
        return

    state = context.user_data.get("state", S_NONE)
    if state != flow:
        await q.edit_message_text("⚠️ Сессия устарела. Начни заново.", reply_markup=None)
        return

    tmp = context.user_data.get("tmp", {})
    kb_reply = admin_keyboard()

    # --- Выбор года ---
    if f"{prefix}year:" in data:
        year = int(data.split(":")[-1])
        tmp["year"] = year
        tmp["step"] = 3
        context.user_data["tmp"] = tmp
        await q.edit_message_text(
            f"Год: {year}\nТеперь выбери месяц:",
            reply_markup=month_keyboard(prefix)
        )
        return

    # --- Выбор месяца ---
    if f"{prefix}month:" in data:
        month_num = int(data.split(":")[-1])
        month_name = NUM_TO_RU[month_num]
        year = int(tmp.get("year", date.today().year))
        amount = int(tmp.get("amount", 0))
        month_key = make_month_key(year, month_num)

        # Сбрасываем состояние
        context.user_data["state"] = S_NONE
        context.user_data.pop("tmp", None)

        target_chat_id = get_active_chat_id(q.from_user.id)
        if not target_chat_id:
            await q.edit_message_text("⚠️ Сначала выбери чат: /manage → «Выбрать чат».")
            return

        # --- Абонемент ---
        if flow == S_ABON:
            title = f"Абонемент {year} {month_name}"
            currency = "PLN"
            await q.edit_message_text(f"⏳ Создаю абонемент: {title}…")
            poll_message = await context.bot.send_poll(
                chat_id=target_chat_id,
                question=title,
                options=["беру ✅", "не беру❌"],
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
                    poll.id, target_chat_id, poll_message.message_id,
                    title, month_key, year, month_num, month_name,
                    currency, amount, poll.options[0].voter_count,
                    None, now_iso(), None
                ))
            await q.edit_message_text(
                f"✅ Создал абонемент: {title}\n💰 Сумма: {amount} {currency}\n\nОтправить итог в чат?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📤 Да, отправить", callback_data=f"sendchat:abon_close:{poll.id}"),
                    InlineKeyboardButton("Нет", callback_data="noop"),
                ]])
            )
            return

        # --- Разовые игры: сумма за месяц ---
        if flow == S_GAMES_TOTAL:
            await q.edit_message_text(f"⏳ Считаю разовые игры за {month_key_to_label(month_key)}…")
            D, users_count = compute_games_total(target_chat_id, month_key, amount)
            if D == 0:
                await q.edit_message_text(
                    "В этом месяце нет разовых игр с участниками ✅ — нечего делить.",
                    reply_markup=None
                )
                return
            result_text = (
                f"✅ Зафиксировал разовые игры за {month_key_to_label(month_key)}\n"
                f"💰 Сумма: {amount} PLN\n"
                f"🎮 Игр с участниками ✅: {D}\n"
                f"👥 Участников с долгом: {users_count}"
            )
            await q.edit_message_text(
                result_text + "\n\nОтправить итог в чат?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📤 Да, отправить", callback_data=f"sendchat:gtotal:{target_chat_id}:{month_key}"),
                    InlineKeyboardButton("Нет", callback_data="noop"),
                ]])
            )
            return


# ---------------- Main ----------------

def main() -> None:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("Укажи BOT_TOKEN в переменной окружения.")
    init_db()

    app = Application.builder().token(token).build()

    # Запоминаем группы, где бот работает (для меню выбора чата в личке)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, remember_chat_handler), group=0)

    app.add_handler(CommandHandler("myid", myid_cmd))

    # Commands
    app.add_handler(CommandHandler("manage", manage_cmd))
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
    app.add_handler(CommandHandler("debtors", debtors_cmd))
    app.add_handler(CommandHandler("export_pretty", export_pretty_cmd))
    app.add_handler(CommandHandler("remind", remind_cmd))

    # Buttons (reply keyboard) + free text router
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_router))

    app.add_handler(CallbackQueryHandler(months_callback, pattern=r"^months\|"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(noop_callback, pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(send_to_chat_callback, pattern=r"^sendchat:"))
    app.add_handler(CallbackQueryHandler(pick_year_month_callback, pattern=r"^(abon|gtotal):(year|month):"))

    # Inline callbacks (months + views + pay toggles)
    app.add_handler(CallbackQueryHandler(callbacks))

    # Poll answers
    app.add_handler(PollAnswerHandler(on_poll_answer))

    threading.Thread(target=_run_health_server, daemon=True).start()
    app.run_polling(allowed_updates=Update.ALL_TYPES)


def _run_health_server() -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            pass  # заглушаем стандартный лог HTTP

    port = int(os.environ.get("PORT", "10000"))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()


if __name__ == "__main__":
    main()
