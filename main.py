import os
import json
import random
import sqlite3
import asyncio
import logging
from math import ceil
from pathlib import Path
from typing import Dict, Any, List, Optional

from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

# ====== CONFIG ======
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise SystemExit("Установите BOT_TOKEN в окружении.")

DB_PATH = "dnd.db"
LOG_FILE = "bot.log"
ADMIN_ID = 478122255  # change if needed

START_GOLD = 30

ATTRIBUTES = ["сила", "ловкость", "интеллект", "внимание", "скрытность", "харизма"]

# Race bonuses (updated attribute names)
RACE_BONUSES = {
    "человек": {"сила": 1,"ловкость": 1,"интеллект": 1,"внимание": 0,"скрытность": 0,"харизма": 0},
    "эльф": {"сила": -1,"ловкость": 2,"интеллект": 0,"внимание": 2,"скрытность": 0,"харизма": 0},
    "дварф": {"сила": 1,"ловкость": -1,"интеллект": 1,"внимание": 0,"скрытность": 2,"харизма": 0},
    "орк": {"сила": 3,"ловкость": -1,"интеллект": -2,"внимание": 2,"скрытность": 0,"харизма": 0}}


# Ensure race dicts include all ATTRIBUTES with default 0
for r, bonuses in list(RACE_BONUSES.items()):
    for a in ATTRIBUTES:
        if a not in bonuses:
            bonuses[a] = 0
    RACE_BONUSES[r] = bonuses

# ====== LOGGING ======
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("dnd_bot")
logging.getLogger("aiogram").setLevel(logging.INFO)

# ====== DB HELPERS ======
def conn():
    return sqlite3.connect(DB_PATH)

def zero_bonus():
    return {a: 0 for a in ATTRIBUTES}

def init_db():
    db_path = Path(DB_PATH).resolve()
    logger.info("Init DB at: %s", db_path)
    c = conn()
    cur = c.cursor()
    # stores table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stores (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 0
    )
    """)
    # items table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        damage INTEGER NOT NULL DEFAULT 0,
        bonus_json TEXT NOT NULL,
        cost INTEGER NOT NULL,
        store_id INTEGER NOT NULL,
        hidden INTEGER DEFAULT 0,
        armor INTEGER DEFAULT 0
    )
    """)
    # characters table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        race TEXT,
        class TEXT,
        attrs TEXT,
        inventory TEXT,
        weapon_id INTEGER,
        armor_id INTEGER,
        gold INTEGER DEFAULT 30,
        hp INTEGER
    )
    """)
    # npc table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS npc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            attrs TEXT NOT NULL,         -- json
            weapon_id INTEGER,
            armor_id INTEGER,
            hp INTEGER NOT NULL,
            in_combat INTEGER NOT NULL DEFAULT 0
        )
        """)
    c.commit()
    c.close()
    seed_stores_and_items_if_empty()
    ensure_flags_table()
    # migrate_characters_defaults()

def seed_stores_and_items_if_empty():
    c = conn()
    cur = c.cursor()
    # seed stores if empty
    cur.execute("SELECT count(*) FROM stores")
    if cur.fetchone()[0] == 0:
        logger.info("Seeding stores")
        stores = [
            (1, "Оружейник", 1),  # default active
            (2, "Бронник", 0),
        ]
        cur.executemany("INSERT INTO stores (id, name, active) VALUES (?,?,?)", stores)
        c.commit()
    # seed items if empty
    cur.execute("SELECT count(*) FROM items")
    if cur.fetchone()[0] == 0:
        logger.info("Seeding items")
        def mb(**kwargs):
            b = zero_bonus()
            for k, v in kwargs.items():
                if k in b:
                    b[k] = v
            return b
        items = [
            # Weapons
            ("Железный короткий меч", "оружие", 1, json.dumps(mb(), ensure_ascii=False), 12, 1, 0, 0),
            ("Тяжёлый боевой топор", "оружие", 2, json.dumps(mb(ловкость=-1, скрытность=-1), ensure_ascii=False), 20, 1, 0, 0),
            ("Лёгкий кинжал", "оружие", 0, json.dumps(mb(), ensure_ascii=False), 8, 1, 0, 0),
            ("Парные ножи", "оружие", 1, json.dumps(mb(), ensure_ascii=False), 14, 1, 0, 0),
            ("Дубовый посох", "оружие", 0, json.dumps(mb(), ensure_ascii=False), 10, 1, 0, 0),
            ("Фокусирующий жезл", "оружие", 1, json.dumps(mb(интеллект=1), ensure_ascii=False), 18, 1, 0, 0),
            ("Охотничий лук", "оружие", 1, json.dumps(mb(), ensure_ascii=False), 15, 1, 0, 0),
            ("Композитный лук", "оружие", 2, json.dumps(mb(), ensure_ascii=False), 22, 1, 0, 0),
            # Armors / accessories
            ("Кольчужная рубаха", "броня", 0, json.dumps(mb(), ensure_ascii=False), 15, 2, 0, 4),
            ("Пояс ярости", "аксессуар", 0, json.dumps(mb(сила=1), ensure_ascii=False), 18, 2, 0, 0),
            ("Теневая куртка", "броня", 0, json.dumps(mb(скрытность=1), ensure_ascii=False), 12, 2, 0, 1),
            ("Перчатки ловкача", "аксессуар", 0, json.dumps(mb(ловкость=1), ensure_ascii=False), 18, 2, 0, 1),
            ("Мантия новичка", "броня", 0, json.dumps(mb(внимание=1), ensure_ascii=False), 12, 2, 0, 1),
            ("Амулет подавления", "аксессуар", 0, json.dumps(mb(), ensure_ascii=False), 20, 2, 0, 0),
            ("Кожаная кираса", "броня", 0, json.dumps(mb(харизма=1), ensure_ascii=False), 12, 2, 0, 1),
            ("Наручи стабилизации", "аксессуар", 0, json.dumps(mb(внимание=1), ensure_ascii=False), 18, 2, 0, 0),
        ]
        cur.executemany(
            "INSERT INTO items (name,type,damage,bonus_json,cost,store_id,hidden,armor) VALUES (?,?,?,?,?,?,?)",
            items
        )
        c.commit()
    c.close()

def migrate_characters_defaults():
    # ensure existing characters have inventory,gold,hp fields set (basic migration)
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT user_id, attrs, weapon, armor, inventory, gold, hp FROM characters")
    rows = cur.fetchall()
    for row in rows:
        user_id, attrs_json, weapon, armor, inv_json, gold, hp = row
        changed = False
        if inv_json is None:
            inv_json = json.dumps([], ensure_ascii=False)
            changed = True
        if gold is None:
            gold = START_GOLD
            changed = True
        if hp is None:
            try:
                attrs = json.loads(attrs_json or "{}")
                strength = int(attrs.get("сила", 0))
                hp = round(strength * 2.2)
            except Exception:
                hp = 0
            changed = True
        if changed:
            cur.execute("UPDATE characters SET inventory=?, gold=?, hp=? WHERE user_id=?", (inv_json, gold, hp, user_id))
    c.commit()
    c.close()

# ---------- NPC HELPERS ----------
def create_npc(name: str, attrs: Dict[str,int], weapon_id: Optional[int], armor_id: Optional[int], hp: int, in_combat: int = 0, damage: int = 0):
    c = conn(); cur = c.cursor()
    cur.execute("""
        INSERT INTO npc (name, attrs, weapon_id, armor_id, hp, in_combat, damage)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (name, json.dumps(attrs, ensure_ascii=False), weapon_id, armor_id, hp, int(in_combat), damage))
    c.commit(); c.close()

def load_npc_full(npc_id: int) -> Optional[Dict[str,Any]]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id, name, attrs, weapon_id, armor_id, hp, in_combat FROM npc WHERE id = ?", (npc_id,))
    row = cur.fetchone(); c.close()
    if not row: return None
    _id, name, attrs_json, weapon_id, armor_id, hp, in_combat = row
    try:
        attrs = json.loads(attrs_json or "{}")
    except Exception:
        attrs = {}
    weapon = get_item_by_id(weapon_id) if weapon_id else None
    armor = get_item_by_id(armor_id) if armor_id else None
    return {
        "id": _id, "name": name, "attrs": attrs,
        "weapon_id": weapon_id, "weapon": weapon,
        "armor_id": armor_id, "armor": armor,
        "hp": hp, "in_combat": bool(in_combat)
    }

def get_npcs_in_combat() -> List[Dict[str,Any]]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM npc WHERE in_combat=1")
    rows = cur.fetchall(); c.close()
    return [load_npc_full(r[0]) for r in rows]

def set_npc_in_combat(npc_id: int, val: bool):
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE npc SET in_combat = ? WHERE id = ?", (1 if val else 0, npc_id))
    c.commit(); c.close()

def apply_damage_to_npc(npc_id: int, incoming_dmg: int) -> Dict[str,Any]:
    """
    Вычитает броню NPC и уменьшает hp. Возвращает dict с keys: effective, new_hp, armor_val, was_killed
    """
    npc = load_npc_full(npc_id)
    if not npc:
        raise ValueError("NPC not found")
    armor_val = 0
    if npc.get("armor_id"):
        arm = get_item_by_id(npc["armor_id"])
        if arm:
            armor_val = int(arm.get("armor") or 0)
    effective = max(0, int(incoming_dmg) - armor_val)
    new_hp = max(0, int(npc["hp"]) - effective)
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE npc SET hp = ? WHERE id = ?", (new_hp, npc_id))
    c.commit(); c.close()
    return {"effective": effective, "new_hp": new_hp, "armor": armor_val, "was_killed": new_hp == 0}

def npc_attack_player(npc_id: int, target_user_id: int) -> Dict[str,Any]:
    npc = load_npc_full(npc_id)
    if not npc:
        raise ValueError("NPC not found")
    weapon_bonus = 0
    if npc.get("weapon"):
        weapon_bonus = int(npc["weapon"].get("damage") or 0)
    roll = random.randint(1,10)
    dmg = roll + weapon_bonus + npc.get("damage",0)
    # target player's armor
    target = load_character_full(target_user_id)
    if not target:
        raise ValueError("Target not found")
    armor_val = 0
    if target.get("armor_id"):
        arm = get_item_by_id(target["armor_id"])
        if arm:
            armor_val = int(arm.get("armor") or 0)
    effective = max(0, dmg - armor_val)
    new_hp = max(0, target.get("hp",0) - effective)
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE characters SET hp = ? WHERE user_id = ?", (new_hp, target_user_id))
    c.commit(); c.close()
    return {"roll": roll, "base_dmg": dmg, "armor": armor_val, "effective": effective, "new_hp": new_hp}


# ====== CHARACTER HELPERS ======
def save_character_full(user_id: int, username: str, race: str, cls: str, attrs: Dict[str,int],
                        inventory: Optional[List[str]] = None, weapon: Optional[str] = None,
                        armor: Optional[str] = None, gold: int = START_GOLD, hp: Optional[int] = None):
    inventory_json = json.dumps(inventory or [], ensure_ascii=False)
    if hp is None:
        strength = attrs.get("сила", 0)
        race_bonus = RACE_BONUSES[race]["сила"]
        hp = round((strength + race_bonus) * 2.2)
        if hp < 10:
            hp = 10
    c = conn()
    cur = c.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO characters (user_id, username, race, class, attrs, hp, inventory, weapon_id, armor_id, gold)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, username, race, cls, json.dumps(attrs, ensure_ascii=False), hp,
          inventory_json, weapon, armor, gold))
    c.commit()
    c.close()
    logger.info("Saved character %s (%s) inv=%s weapon=%s armor=%s gold=%s hp=%s",
                username, user_id, inventory_json, weapon, armor, gold, hp)

def load_all_characters() -> List[Dict[str, Any]]:
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT user_id, username, race, class, attrs, inventory, weapon_id, armor_id, gold, hp FROM characters")
    rows = cur.fetchall()
    c.close()
    res = []
    for (user_id, username, race, cls, attrs_json, inv_json, weapon_id, armor_id, gold, hp) in rows:
        attrs = json.loads(attrs_json or "{}")
        # inventory ids -> names
        inv_ids = json.loads(inv_json or "[]")
        inv_names = []
        for iid in inv_ids:
            item = get_item_by_id(iid)
            if item:
                inv_names.append(item["name"])
            else:
                inv_names.append(f"<missing id:{iid}>")
        # weapon
        weapon = get_item_by_id(weapon_id) if weapon_id else None
        armor = get_item_by_id(armor_id) if armor_id else None
        # weapon/armor convenient fields
        weapon_name = weapon["name"] if weapon else None
        weapon_damage = int(weapon["damage"]) if weapon and weapon.get("damage") is not None else 0
        armor_name = armor["name"] if armor else None
        armor_value = int(armor.get("armor") or 0) if armor else 0

        # build record
        res.append({
            "user_id": user_id,
            "username": username,
            "race": race,
            "class": cls,
            "attrs": attrs,
            "inventory_ids": inv_ids,
            "inventory_names": inv_names,
            "weapon_id": weapon_id,
            "weapon_name": weapon_name,
            "weapon_damage": weapon_damage,
            "armor_id": armor_id,
            "armor_name": armor_name,
            "armor_value": armor_value,
            "gold": gold,
            "hp": hp
        })
    return res

def load_character_full(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Загружает персонажа. Ожидается, что characters.inventory хранит JSON-массив ID (ints).
    Возвращает расширённую структуру с именами/значениями экипированных предметов.
    """
    c = conn()
    cur = c.cursor()
    cur.execute(
        "SELECT username, race, class, attrs, inventory, weapon_id, armor_id, gold, hp FROM characters WHERE user_id = ?",
        (user_id,)
    )
    row = cur.fetchone()
    c.close()
    if not row:
        return None

    username, race, cls, attrs_json, inv_json, weapon_id, armor_id, gold, hp = row

    # attrs
    try:
        attrs = json.loads(attrs_json or "{}")
    except Exception:
        attrs = {}

    # inventory_ids (assume list of ints)
    try:
        inventory_ids = json.loads(inv_json or "[]")
        if not isinstance(inventory_ids, list):
            inventory_ids = []
    except Exception:
        inventory_ids = []

    # inventory names from ids
    inventory_names: List[str] = []
    for iid in inventory_ids:
        item = get_item_by_id(iid)
        inventory_names.append(item["name"] if item else f"<missing id:{iid}>")

    # weapon (by id)
    weapon = get_item_by_id(weapon_id) if weapon_id else None
    weapon_name = weapon["name"] if weapon else None
    try:
        weapon_damage = int(weapon.get("damage") or 0) if weapon else 0
    except Exception:
        weapon_damage = 0

    # armor (by id)
    armor = get_item_by_id(armor_id) if armor_id else None
    armor_name = armor["name"] if armor else None
    try:
        armor_value = int(armor.get("armor") or 0) if armor else 0
    except Exception:
        armor_value = 0

    # compute max_hp using strength + racial bonus
    strength = int(attrs.get("сила", 0))
    race_bonus_strength = int(RACE_BONUSES.get(race, {}).get("сила", 0) or 0)
    max_hp = ceil((strength + race_bonus_strength) * 2.2)

    return {
        "user_id": user_id,
        "username": username,
        "race": race,
        "class": cls,
        "attrs": attrs,
        "inventory_ids": inventory_ids,
        "inventory_names": inventory_names,
        "weapon_id": weapon_id,
        "weapon": weapon_name,
        "weapon_damage": weapon_damage,
        "armor_id": armor_id,
        "armor": armor_name,
        "armor_value": armor_value,
        "gold": gold or 0,
        "hp": hp or 0,
        "max_hp": max_hp
    }


# ====== ITEMS HELPERS ======

def get_item_by_id(item_id: int) -> Optional[Dict[str, Any]]:
    if item_id is None:
        return None
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT id, name, type, damage, bonus_json, cost, store_id, armor FROM items WHERE id = ?", (item_id,))
    row = cur.fetchone()
    c.close()
    if not row:
        return None
    _id, name, typ, dmg, bonus_json, cost, store_id, armor = row
    bonus = json.loads(bonus_json) if bonus_json else zero_bonus()
    return {"id": _id, "name": name, "type": typ, "damage": dmg, "bonus": bonus, "cost": cost, "store_id": store_id, "armor": armor}

def get_all_items_active_store() -> List[Dict[str, Any]]:
    # returns items for active store(s) - but we ensure only one active
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT id, name, type, damage, bonus_json, cost, store_id, armor FROM items WHERE hidden = 0 AND store_id IN (SELECT id FROM stores WHERE active=1)")
    rows = cur.fetchall()
    c.close()
    res = []
    for r in rows:
        _id, name, typ, dmg, bonus_json, cost, store_id, armor = r
        bonus = json.loads(bonus_json) if bonus_json else zero_bonus()
        res.append({"id": _id, "name": name, "type": typ, "damage": dmg, "bonus": bonus, "cost": cost, "store_id": store_id, "armor":armor})
    return res

def get_item_by_name(name: str) -> Optional[Dict[str, Any]]:
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT id, name, type, damage, bonus_json, cost, store_id, armor FROM items WHERE name = ?", (name,))
    row = cur.fetchone()
    c.close()
    if not row:
        return None
    _id, name, typ, dmg, bonus_json, cost, store_id, armor = row
    bonus = json.loads(bonus_json) if bonus_json else zero_bonus()
    return {"id": _id, "name": name, "type": typ, "damage": dmg, "bonus": bonus, "cost": cost, "store_id": store_id, "armor":armor}

def get_active_store() -> Optional[Dict[str, Any]]:
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT id, name FROM stores WHERE active=1")
    row = cur.fetchone()
    c.close()
    if not row:
        return None
    return {"id": row[0], "name": row[1]}

def set_active_store(store_id: int):
    c = conn()
    cur = c.cursor()
    cur.execute("UPDATE stores SET active = CASE WHEN id = ? THEN 1 ELSE 0 END", (store_id,))
    c.commit()
    c.close()

def ensure_flags_table():
    c = conn()
    cur = c.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flags (
        name TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    )
    """)
    # seed default if not exists
    cur.execute("INSERT OR IGNORE INTO flags (name, value) VALUES ('shop_enabled', 1)")
    c.commit()
    c.close()

def set_flag(name: str, val: int):
    c = conn()
    cur = c.cursor()
    cur.execute("INSERT INTO flags (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value", (name, val))
    c.commit()
    c.close()

def get_flag(name: str) -> int:
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT value FROM flags WHERE name = ?", (name,))
    r = cur.fetchone()
    c.close()
    return r[0] if r else 0

# ====== UI utils ======
def chunked_list(lst: List, n: int) -> List[List]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def make_keyboard_from_options(options: List[str], cols: int = 2) -> ReplyKeyboardMarkup:
    if not options:
        return ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    rows = chunked_list([KeyboardButton(text=o) for o in options], cols)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def make_keyboard_numbers(max_n: int, min_n: int = 0) -> ReplyKeyboardMarkup:
    nums = [str(i) for i in range(min_n, max_n + 1)]
    if not nums:
        nums = ["0"]
    count = len(nums)
    cols = max(1, ceil(count / 2))
    rows = []
    for i in range(0, count, cols):
        row = [KeyboardButton(text=n) for n in nums[i:i+cols]]
        rows.append(row)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def main_menu_keyboard(user_id: int, chat_type: str) -> ReplyKeyboardMarkup:
    base = []
    if chat_type == "private":
        char = load_character_full(user_id)
        if char:
            base.append(KeyboardButton(text="Пересоздать персонажа"))
        else:
            base.append(KeyboardButton(text="Создать персонажа"))
        base.append(KeyboardButton(text="Показать персонажа"))
        base.append(KeyboardButton(text="Экипировка"))
        if user_id == ADMIN_ID:
            base.append(KeyboardButton(text="Персонажи"))
            base.append(KeyboardButton(text="Игроки"))
            base.append(KeyboardButton(text="Магазины"))
            # кнопка управления показом магазина
            shop_on = get_flag("shop_enabled")
            base.append(KeyboardButton(text=f"Показ магазина: {'Вкл' if shop_on else 'Выкл'}"))
    else:
        # group menu
        base.append(KeyboardButton(text="Показать персонажа"))
        base.append(KeyboardButton(text="Товары"))
        base.append(KeyboardButton(text="Испытание"))
        base.append(KeyboardButton(text="Урон"))
        base.append(KeyboardButton(text="Мобы"))
    rows = chunked_list(base, 2)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=False)


# ====== SESSIONS ======
CREATION_SESSIONS: Dict[int, Dict[str, Any]] = {}
EQUIP_SESSIONS: Dict[int, Dict[str, Any]] = {}
GM_SESSIONS: Dict[int, Dict[str, Any]] = {}
COMBAT_SESSIONS: Dict[int, Dict[str, Any]] = {}
GM_COMBAT_SESSIONS: Dict[int, Dict[str, Any]] = {}

# ====== Aiogram init ======
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== COMMANDS ======
@dp.message(Command(commands=["start"]))
async def cmd_start(message: Message):
    logger.info("start from %s (%s)", message.from_user.username, message.from_user.id)
    if message.chat.type == "private":
        await message.answer(
        "Привет! DnD бот.\nСоздать персонажа — /create\nПоказать персонажа — /show\nЭкипировка — /equip",
        reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type)
        )
    else:
        await message.answer(
            "Привет! DnD бот.\nПоказать персонажа — /show\nТовары — /shop\nУрон — /attack ",
            reply_markup=main_menu_keyboard(message.from_user.id, message.chat.type))

@dp.message(Command(commands=["create"]))
async def cmd_create(message: Message):
    if message.chat.type != "private":
        await message.answer("Создавать персонажа в личке.")
        return
    CREATION_SESSIONS[message.from_user.id] = {"step": "race"}
    race_keys = list(RACE_BONUSES.keys())
    race_labels = [f"{r} ({', '.join([k+('+' if v>0 else '')+str(v) for k,v in RACE_BONUSES[r].items() if v!=0])})" for r in race_keys]
    LABEL_TO_RACE = {label: key for label, key in zip(race_labels, race_keys)}
    CREATION_SESSIONS[message.from_user.id]["race_label_map"] = LABEL_TO_RACE
    await message.answer("Выберите расу:", reply_markup=make_keyboard_from_options(race_labels, cols=2))

@dp.message(Command(commands=["show"]))
async def cmd_show(message: Message):
    char = load_character_full(message.from_user.id)
    if not char:
        await message.answer("Персонаж не найден. Создайте: /create", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
        return

    weapon_name = char.get("weapon")
    weapon_dmg = char.get("weapon_damage")
    armor_name = char.get("armor")
    armor_val = char.get("armor_value")
    lines = []
    lines.append("Персонаж сохранён.")
    lines.append(f"Персонаж @{char['username']} (id {char['user_id']})")
    lines.append(f"Раса: {char['race']}, Класс: {char['class']}")
    lines.append("Атрибуты (без бонусов расы):")
    rb = RACE_BONUSES.get(char['race'], {})
    for a in ATTRIBUTES:
        base = char['attrs'].get(a, 0)
        br = rb.get(a, 0)
        if br:
            sign = "+" if br>0 else ""
            lines.append(f"  {a}: {base} ({sign}{br})")
        else:
            lines.append(f"  {a}: {base}")
    lines.append(f"HP: {char.get('hp')}")
    lines.append(f"Золото: {char.get('gold',0)}")
    equip_parts = []

    if weapon_name:
        equip_parts.append(f"Оружие — {weapon_name} ({weapon_dmg} урона)")
    if armor_name:
        equip_parts.append(f"Броня — {armor_name} ({armor_val} брони)")

    if equip_parts:
        lines.append("Экипировано: " + ", ".join(equip_parts))
    else:
        lines.append("Экипировано: ничего")

    inv = char.get('inventory_names') or []
    lines.append("Инвентарь: " + (", ".join(inv) if inv else "-"))
    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))

@dp.message(Command(commands=["shop"]))
async def cmd_shop(message: Message):
    if not get_flag("shop_enabled"):
        await message.answer("Магазин сейчас закрыт.",
                       reply_markup=main_menu_keyboard(message.from_user.id, message.chat.type))
        return
    active = get_active_store()
    if not active:
        await message.answer("Магазин не активен. Обратитесь к администратору.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
        return
    items = get_all_items_active_store()
    weapons = [i["name"] for i in items if i["type"] == "оружие"]
    armors = [i["name"] for i in items if i["type"] in ("броня","аксессуар")]
    msg = []
    msg.append(f"Магазин: {active['name']}\n")
    if weapons:
        msg.append("Оружие:")
        msg += [f"• {n}" for n in weapons]
    if armors:
        msg.append("\nБроня/Аксессуары:")
        msg += [f"• {n}" for n in armors]
    await message.answer("\n".join(msg), reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))

@dp.message(Command(commands=["equip"]))
async def cmd_equip(message: Message):
    if message.chat.type != "private":
        return
    user_id = message.from_user.id
    char = load_character_full(user_id)
    if not char:
        await message.answer("Персонаж не найден. Создайте: /create", reply_markup=main_menu_keyboard(user_id,message.chat.type))
        return
    EQUIP_SESSIONS[user_id] = {"step": "choose_type"}
    kb = make_keyboard_from_options(["Оружие", "Броня"], cols=2)
    await message.answer("Что экипировать?", reply_markup=kb)

@dp.message(Command(commands=["attack"]))
async def cmd_attack(message: Message):
    user_id = message.from_user.id
    npcs = get_npcs_in_combat()
    if not npcs:
        await message.answer("Сейчас нет мобов в бою.", reply_markup=main_menu_keyboard(user_id,message.chat.type))
        return
    names = [n["name"] for n in npcs]
    # сохраним мап в сессии для дальнейшего выбора
    COMBAT_SESSIONS[user_id] = {"step": "player_choose_npc", "npcs": {n["name"]: n["id"] for n in npcs}}
    await message.answer("Выберите моба, которому нанеcёте урон:", reply_markup=make_keyboard_from_options(names, cols=2))
    return

@dp.message(Command(commands=["list"]))
async def cmd_list(message: Message):
    logger.info("/list from %s (%s)", message.from_user.username, message.from_user.id)
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас нет прав для выполнения этой команды.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
        return
    all_chars = load_all_characters()
    if not all_chars:
        await message.answer("Персонажей нет.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
        return
    lines = []
    for c in all_chars:
        weapon_name = c['weapon_name'] or "-"
        armor_name = c['armor_name'] or "-"
        lines.append(
            f"@{c['username']}: "
            f"{c['race']} / {c['class']} — "
            f"урон оружия: {c['weapon_damage']}, броня: {c['armor_value']}"
        )
    # отправляем по кускам, если много
    chunk_size = 40
    for i in range(0, len(lines), chunk_size):
        await message.answer("\n".join(lines[i:i+chunk_size]), reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))

# ====== UNIVERSAL HANDLER (creation, equip, equip choose_item, GM flows, etc.) ======
@dp.message()
async def universal_handler(message: Message):
    try:
        # allow command handlers to process commands
        if message.entities:
            for ent in message.entities:
                if ent.type == "bot_command":
                    return

        user_id = message.from_user.id
        text = (message.text or "").strip()

        if user_id in COMBAT_SESSIONS and COMBAT_SESSIONS[user_id].get("step") == "player_choose_npc":
            sel = text
            m = COMBAT_SESSIONS[user_id]["npcs"]
            if sel not in m:
                await message.answer("Неверный выбор.", reply_markup=main_menu_keyboard(user_id, message.chat.type))
                COMBAT_SESSIONS.pop(user_id, None);
                return
            npc_id = m[sel]
            # compute player's damage
            char = load_character_full(user_id)
            if not char:
                await message.answer("Персонаж не найден.")
                COMBAT_SESSIONS.pop(user_id, None);
                return
            weapon_bonus = int(char.get("weapon_damage") or 0)
            roll = random.randint(1, 10)
            total = roll + weapon_bonus
            res = apply_damage_to_npc(npc_id, total)
            msg = f"🎲 d10: {roll} + оружие {weapon_bonus} = {total}\nБроня моба: {res['armor']} -> эффективный урон {res['effective']}. Осталось HP: {res['new_hp']}"
            if res["was_killed"]:
                msg += f"\n{sel} погиб."
                set_npc_in_combat(npc_id, False)
            await message.answer(msg, reply_markup=main_menu_keyboard(user_id, message.chat.type))
            COMBAT_SESSIONS.pop(user_id, None)
            return

        if user_id in GM_COMBAT_SESSIONS and GM_COMBAT_SESSIONS[user_id].get("step") == "admin_choose_npc":
            logger.info("STADIA %S",GM_COMBAT_SESSIONS[user_id].get("step"))
            sel = text
            mp = GM_COMBAT_SESSIONS[user_id]["map"]
            if sel not in mp:
                await message.answer("Неверный выбор.", reply_markup=main_menu_keyboard(user_id, message.chat.type))
                GM_COMBAT_SESSIONS.pop(user_id, None);
                return
            npc_id = mp[sel]
            GM_COMBAT_SESSIONS[user_id] = {"step": "admin_npc_actions", "npc_id": npc_id}
            await message.answer("Действие:",
                           reply_markup=make_keyboard_from_options(["Испытание", "Урон", "Отмена"], cols=2))
            return

        if user_id in GM_COMBAT_SESSIONS and GM_COMBAT_SESSIONS[user_id].get("step") == "admin_npc_actions":
            logger.info("STADIA %S",GM_COMBAT_SESSIONS[user_id].get("step"))
            logger.info("ya zeliboba")

            action = text
            if action == "Отмена":
                GM_COMBAT_SESSIONS.pop(user_id, None);
                await message.answer("Отменено.", reply_markup=main_menu_keyboard(user_id, message.chat.type));
                return
            npc_id = GM_COMBAT_SESSIONS[user_id]["npc_id"]
            npc = load_npc_full(npc_id)
            if action == "Испытание":
                logger.info("abiba")
                # показываем атрибуты кнопками (или спрашиваем какой атрибут)
                GM_COMBAT_SESSIONS[user_id]["step"] = "admin_npc_choose_attr"
                GM_COMBAT_SESSIONS[user_id]["npc_id"] = npc_id
                await message.answer("Выберите атрибут для испытания:",
                               reply_markup=make_keyboard_from_options(ATTRIBUTES, cols=3))
                return
            if action == "Урон":
                # показ игроков-целей
                players = load_all_characters()
                if not players:
                    await message.answer("Нет игроков.", reply_markup=main_menu_keyboard(user_id, message.chat.type));
                    GM_COMBAT_SESSIONS.pop(user_id, None);
                    return
                labels = [f"{p['username']} ({p['user_id']})" for p in players]
                GM_COMBAT_SESSIONS[user_id]["step"] = "admin_npc_choose_player"
                GM_COMBAT_SESSIONS[user_id]["npc_id"] = npc_id
                GM_COMBAT_SESSIONS[user_id]["players_map"] = {labels[i]: players[i]["user_id"] for i in range(len(players))}
                await message.answer("Выберите игрока для атаки:", reply_markup=make_keyboard_from_options(labels, cols=2))
                return

        if user_id in GM_COMBAT_SESSIONS and GM_COMBAT_SESSIONS[user_id].get("step") == "admin_npc_choose_attr":
            attr = text
            if attr not in ATTRIBUTES:
                await message.answer("Неверный атрибут.", reply_markup=main_menu_keyboard(user_id, message.chat.type))
                GM_COMBAT_SESSIONS.pop(user_id, None);
                return
            logger.info("i'm here")
            npc_id = GM_COMBAT_SESSIONS[user_id]["npc_id"]
            npc = load_npc_full(npc_id)
            base = int(npc["attrs"].get(attr, 0))
            logger.info("base npc attr %s",base)
            weapon_bonus = 0
            armor_bonus = 0
            weapon_id = npc.get("weapon_id")
            armor_id = npc.get("armor_id")
            if weapon_id:
                weapon = get_item_by_id(weapon_id)
                weapon_bonus = int(weapon.get("bonus").get(text, 0))
            if armor_id:
                armor = get_item_by_id(armor_id)
                armor_bonus = int(armor.get("bonus").get(text, 0))
            roll = random.randint(1, 20)
            total = roll + base + armor_bonus + weapon_bonus
            await message.answer(f"NPC {npc['name']} бросок d20: {roll}\nАтрибут {attr}: {base}\nИтого: {total}",
                           reply_markup=main_menu_keyboard(user_id, message.chat.type))
            GM_COMBAT_SESSIONS.pop(user_id, None);
            return

        if user_id in GM_COMBAT_SESSIONS and GM_COMBAT_SESSIONS[user_id].get("step") == "admin_npc_choose_player":
            sel = text
            pm = GM_COMBAT_SESSIONS[user_id].get("players_map", {})
            if sel not in pm:
                await message.answer("Неверный игрок.", reply_markup=main_menu_keyboard(user_id, message.chat.type));
                GM_COMBAT_SESSIONS.pop(user_id, None);
                return
            target_id = pm[sel]
            npc_id = GM_COMBAT_SESSIONS[user_id]["npc_id"]
            res = npc_attack_player(npc_id, target_id)
            target = load_character_full(target_id)
            npc = load_npc_full(npc_id)
            await message.answer(
                f"NPC {npc['name']} атаковал {target['username']}: d10 {res['roll']} -> базовый урон {res['base_dmg']}. "
                f"Броня цели {res['armor']} -> эффективный урон {res['effective']}. HP цели: {res['new_hp']}",
                reply_markup=main_menu_keyboard(user_id, message.chat.type)
            )
            GM_COMBAT_SESSIONS.pop(user_id, None)
            return

        if message.chat.type == "private" and user_id == ADMIN_ID and text.startswith("Показ магазина:"):
            current = get_flag("shop_enabled")
            new = 0 if current else 1
            set_flag("shop_enabled", new)
            await message.answer(f"Показ товаров {'включён' if new else 'выключен'}.",
                           reply_markup=main_menu_keyboard(user_id, message.chat.type))
            return

        if text == "Урон" and message.chat.type in ("group", "supergroup"):
            await cmd_attack(message)
            return

        if text == "Испытание" and message.chat.type in ("group", "supergroup"):
            kb = make_keyboard_from_options(ATTRIBUTES, cols=3)  # 3 колонки -> 2 ряда
            await message.answer("Выберите атрибут для испытания:", reply_markup=kb)
            return

            # Обработка нажатия на атрибут в групповом чате

        if message.chat.type in ("group", "supergroup") and text in ATTRIBUTES:
            char = load_character_full(user_id)
            if not char:
                await message.answer("Персонаж не найден. Создайте в личке: Создать персонажа")
                return
            base = int(char["attrs"].get(text, 0))
            race_bonus = int(RACE_BONUSES.get(char["race"], {}).get(text, 0) or 0)
            weapon_bonus = 0
            armor_bonus = 0
            weapon_id = char.get("weapon_id")
            armor_id = char.get("armor_id")
            if weapon_id:
                weapon = get_item_by_id(weapon_id)
                weapon_bonus = int(weapon.get("bonus").get(text, 0))
            if armor_id:
                armor = get_item_by_id(armor_id)
                armor_bonus = int(armor.get("bonus").get(text,0))
            roll = random.randint(1, 20)
            total = roll + base + race_bonus + weapon_bonus + armor_bonus
            await message.answer(
                f"🎲 {message.from_user.first_name} бросок d20: {roll}\n"
                f"Атрибут {text}: {base} (бонус {race_bonus:+d})\n"
                f"Итого: {total}",reply_markup=main_menu_keyboard(user_id,message.chat.type)
            )
            return

        if text == "Мобы" and message.chat.type in ("group", "supergroup") and user_id == ADMIN_ID:
            c = conn();
            cur = c.cursor()
            cur.execute("SELECT id, name FROM npc where in_combat = 1 ")
            rows = cur.fetchall();
            c.close()
            if not rows:
                await message.answer("Мобов нет.", reply_markup=main_menu_keyboard(user_id, message.chat.type));
                return
            labels = [r[1] for r in rows]
            GM_COMBAT_SESSIONS[user_id] = {"step": "admin_choose_npc", "map": {r[1]: r[0] for r in rows}}
            logger.info("STADIA %S",GM_COMBAT_SESSIONS[user_id].get("step"))
            await message.answer("Выберите моба:", reply_markup=make_keyboard_from_options(labels, cols=2))
            return

        if text == "Создать персонажа":
            await cmd_create(message)
            return

        if text == "Пересоздать персонажа":
            await cmd_create(message)
            return

        if text == "Показать персонажа":
            await cmd_show(message)
            return

        if text == "Экипировка":
            await cmd_equip(message)
            return

        if text == "Товары":
            if message.chat.type in ("group", "supergroup"):
                if not get_flag("shop_enabled"):
                    await message.answer("Магазин сейчас закрыт.",
                                   reply_markup=main_menu_keyboard(user_id, message.chat.type))
                    return
            await cmd_shop(message)
            return

        if text == "Персонажи":
            await cmd_list(message)
            return

        # Creation flow
        if message.chat.type == "private" and user_id in CREATION_SESSIONS:
            session = CREATION_SESSIONS[user_id]
            step = session.get("step")
            if step == "race":
                label_map = session.get("race_label_map", {})
                selected_key = label_map.get(message.text, message.text.lower())
                if selected_key not in RACE_BONUSES:
                    race_keys = list(RACE_BONUSES.keys())
                    race_labels = [f"{r} ({', '.join([k+('+' if v>0 else '')+str(v) for k,v in RACE_BONUSES[r].items() if v!=0])})" for r in race_keys]
                    await message.answer("Неверная раса. Выберите ещё раз.", reply_markup=make_keyboard_from_options(race_labels, cols=2))
                    return
                session["race"] = selected_key
                session["step"] = "class"
                await message.answer("Выберите класс:", reply_markup=make_keyboard_from_options(["воин","вор","волшебник","лучник"], cols=2))
                return
            if step == "class":
                if text.lower() not in ("воин","вор","волшебник","лучник"):
                    await message.answer("Неверный класс. Выберите из кнопок.", reply_markup=make_keyboard_from_options(["воин","вор","волшебник","лучник"], cols=2))
                    return
                session["class"] = text.lower()
                session["step"] = "alloc"
                session["attrs_list"] = ATTRIBUTES.copy()
                session["index"] = 0
                session["remaining"] = 10
                session["allocs"] = {}

                attrs_display = "\n".join([f"- {i + 1}. {attr}" for i, attr in enumerate(session["attrs_list"])])
                await message.answer(
                    "Сейчас вы будете распределять 10 очков между атрибутами.\n\n"
                    "Атрибуты:\n"
                    f"{attrs_display}\n\n"
                    "Важно: потратить нужно все 10 очков. Если останутся — начнём заново."# опционально: можно убрать клавиатуру здесь
                )

                current_attr = session["attrs_list"][0]
                await message.answer(f"Осталось {session['remaining']} очков. Сколько в {current_attr}?", reply_markup=make_keyboard_numbers(session["remaining"], 0))
                return
            if step == "alloc":
                try:
                    val = int(text)
                except ValueError:
                    current_attr = session["attrs_list"][session["index"]]
                    await message.answer(f"Выберите число кнопкой. Сколько в {current_attr}?", reply_markup=make_keyboard_numbers(session["remaining"], 0))
                    return
                if val < 0 or val > session["remaining"]:
                    current_attr = session["attrs_list"][session["index"]]
                    await message.answer(f"Неверно. Выберите 0..{session['remaining']} для {current_attr}.", reply_markup=make_keyboard_numbers(session["remaining"], 0))
                    return
                attr = session["attrs_list"][session["index"]]
                session["allocs"][attr] = val
                session["remaining"] -= val
                session["index"] += 1
                if session["index"] < len(session["attrs_list"]):
                    next_attr = session["attrs_list"][session["index"]]
                    await message.answer(f"Осталось {session['remaining']} очков. Сколько в {next_attr}?", reply_markup=make_keyboard_numbers(session["remaining"], 0))
                    return
                if session["remaining"] > 0:
                    leftover = session["remaining"]
                    session["allocs"] = {}
                    session["index"] = 0
                    session["remaining"] = 10
                    first_attr = session["attrs_list"][0]
                    await message.answer(f"Вы не потратили все очки (осталось {leftover}). Начинаем заново. Сколько в {first_attr}?", reply_markup=make_keyboard_numbers(session["remaining"], 0))
                    return
                final_attrs = {a: session["allocs"].get(a, 0) for a in ATTRIBUTES}
                starter_inventory = [1,9]
                save_character_full(user_id, message.from_user.username or message.from_user.full_name,
                                    session["race"], session["class"], final_attrs,
                                    inventory=starter_inventory, weapon=None, armor=None, gold=START_GOLD)
                CREATION_SESSIONS.pop(user_id, None)
                await message.answer("Персонаж сохранён.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                return

        # EQUIP flow
        if user_id in EQUIP_SESSIONS:
            ses = EQUIP_SESSIONS[user_id]
            step = ses.get("step")

            # --------------------------------------------------------
            # 1) Выбор типа (оружие / броня)
            # --------------------------------------------------------
            if step == "choose_type":
                typ = text.lower()
                if typ not in ("оружие", "броня"):
                    await message.answer(
                        "Выберите 'Оружие' или 'Броня'.",
                        reply_markup=make_keyboard_from_options(["Оружие", "Броня"], cols=2)
                    )
                    return

                char = load_character_full(user_id)
                inv_ids = char.get("inventory_ids", [])

                if not inv_ids:
                    await message.answer("У вас нет предметов в инвентаре.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    EQUIP_SESSIONS.pop(user_id, None)
                    return

                # найдем предметы нужного типа по ID
                c = conn()
                cur = c.cursor()
                placeholders = ",".join("?" * len(inv_ids))
                query = f"SELECT id, name FROM items WHERE id IN ({placeholders}) AND type = ?"
                params = inv_ids + [typ]
                cur.execute(query, params)
                rows = cur.fetchall()
                c.close()

                if not rows:
                    await message.answer(f"У вас нет предметов типа {typ}.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    EQUIP_SESSIONS.pop(user_id, None)
                    return

                # candidates: id → name mapping
                candidates_ids = [r[0] for r in rows]
                candidates_names = [r[1] for r in rows]

                ses["step"] = "choose_item"
                ses["type"] = typ
                ses["candidates_ids"] = candidates_ids
                ses["candidates_names"] = candidates_names

                await message.answer(
                    "Выберите предмет:",
                    reply_markup=make_keyboard_from_options(candidates_names, cols=2)
                )
                return

            # --------------------------------------------------------
            # 2) Выбор конкретного предмета
            # --------------------------------------------------------
            if step == "choose_item":
                chosen_name = text
                ses = EQUIP_SESSIONS[user_id]

                if chosen_name not in ses.get("candidates_names", []):
                    await message.answer(
                        "Неверный выбор. Отмена.",
                        reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type)
                    )
                    EQUIP_SESSIONS.pop(user_id, None)
                    return

                # Находим id выбранного предмета
                idx = ses["candidates_names"].index(chosen_name)
                chosen_id = ses["candidates_ids"][idx]

                typ = ses["type"]
                char = load_character_full(user_id)

                if not char:
                    await message.answer("Персонаж не найден.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    EQUIP_SESSIONS.pop(user_id, None)
                    return

                inventory = char["inventory_ids"][:]  # список ID
                current_weapon_id = char.get("weapon_id")
                current_armor_id = char.get("armor_id")

                # 1) Возвращаем старое оружие/броню в инвентарь
                if typ == "оружие":
                    if current_weapon_id:
                        inventory.append(current_weapon_id)
                else:  # броня
                    if current_armor_id:
                        inventory.append(current_armor_id)

                # 2) Убираем выбранный предмет из инвентаря
                if chosen_id in inventory:
                    inventory.remove(chosen_id)

                # 3) Обновляем weapon_id / armor_id
                new_weapon_id = chosen_id if typ == "оружие" else current_weapon_id
                new_armor_id = chosen_id if typ == "броня" else current_armor_id

                c = conn()
                cur = c.cursor()
                cur.execute("""
                    UPDATE characters SET
                    weapon_id = ?, armor_id = ?, inventory = ?
                    WHERE user_id = ?
                """, (
                    new_weapon_id,
                    new_armor_id,
                    json.dumps(inventory, ensure_ascii=False),
                    user_id
                ))
                c.commit()
                c.close()

                EQUIP_SESSIONS.pop(user_id, None)
                await message.answer(
                    f"{chosen_name} успешно экипирован(а).",
                    reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type)
                )
                return

        # GM (admin) flows via button "Игроки" (only in groups)
        if message.chat.type in ("private") and user_id == ADMIN_ID:
            # start admin players menu

            if text == "Игроки":
                if message.chat.type != "private":
                    return
                # list all characters
                c = conn()
                cur = c.cursor()
                cur.execute("SELECT user_id, username FROM characters")
                rows = cur.fetchall()
                c.close()
                if not rows:
                    await message.answer("Персонажей нет.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    return
                names = [f"{r[1]} ({r[0]})" for r in rows]
                # save mapping
                GM_SESSIONS[user_id] = {"step": "choose_player", "players_map": {f"{r[1]} ({r[0]})": r[0] for r in rows}}
                await message.answer("Выберите игрока:", reply_markup=make_keyboard_from_options(names, cols=2))
                return
            # choose player
            if user_id in GM_SESSIONS:
                gs = GM_SESSIONS[user_id]
                gstep = gs.get("step")
                if gstep == "choose_player":
                    if message.chat.type != "private":
                        return
                    sel = text
                    players_map = gs.get("players_map", {})
                    if sel not in players_map:
                        await message.answer("Неверный игрок.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    target_id = players_map[sel]
                    gs["step"] = "chosen_player"
                    gs["target_id"] = target_id
                    # actions
                    await message.answer("Действие для игрока:", reply_markup=make_keyboard_from_options(["Урон","Торговля","Лечение","Здоровье","Отмена"], cols=2))
                    return
                if gstep == "chosen_player":
                    if message.chat.type != "private":
                        return
                    action = text
                    if action == "Отмена":
                        GM_SESSIONS.pop(user_id, None)
                        await message.answer("Отменено.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        return
                    if action == "Урон":
                        gs["step"] = "gm_input_damage"
                        await message.answer("Введи количество урона (целое число):", reply_markup=make_keyboard_from_options(["Отмена"], cols=1))
                        return
                    if action == "Лечение":
                        gs["step"] = "gm_input_heal"
                        await message.answer("Введи количество лечения (целое число):", reply_markup=make_keyboard_from_options(["Отмена"], cols=1))
                        return
                    if action == "Здоровье":
                        target_id = gs.get("target_id")
                        char = load_character_full(target_id)
                        if not char:
                            await message.answer("Игрок не найден.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                            GM_SESSIONS.pop(user_id, None)
                            return
                        strength = char["attrs"].get("сила", 0)
                        race_bonus_strength = int(RACE_BONUSES.get(char.get('race'), {}).get("сила", 0) or 0)
                        max_hp = round((strength + race_bonus_strength) * 2.2)
                        if max_hp < 5:
                            max_hp = 10
                        c = conn()
                        cur = c.cursor()
                        cur.execute("UPDATE characters SET hp = ? WHERE user_id = ?", (max_hp, target_id))
                        c.commit()
                        c.close()
                        await message.answer(f"Игрок {char['username']} вылечен полностью ({max_hp} HP).", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    if action == "Торговля":
                        # show active store items to give to target player (admin buys it from store and it will be added to player's inventory if they have gold)
                        active = get_active_store()
                        if not active:
                            await message.answer("Активный магазин не выбран.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                            GM_SESSIONS.pop(user_id, None)
                            return
                        items = get_all_items_active_store()
                        if not items:
                            await message.answer("В магазине нет предметов.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                            GM_SESSIONS.pop(user_id, None)
                            return
                        # prepare list and mapping
                        item_names = [it["name"] + f" ({it['cost']}g)" for it in items]
                        GS_MAP = {item_names[i]: items[i] for i in range(len(items))}
                        gs["step"] = "gm_trade_choose"
                        gs["trade_map"] = GS_MAP
                        await message.answer("Выберите предмет для продажи игроку (админ платит):", reply_markup=make_keyboard_from_options(item_names, cols=2))
                        return
                if gstep == "gm_input_damage":
                    if message.chat.type != "private":
                        return
                    if text == "Отмена":
                        GM_SESSIONS.pop(user_id, None)
                        await message.answer("Отменено.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        return
                    try:
                        dmg = int(text)
                    except ValueError:
                        await message.answer("Введите целое число урона.")
                        return
                    target_id = gs.get("target_id")
                    char = load_character_full(target_id)
                    if not char:
                        await message.answer("Игрок не найден.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    armor_name = char.get("armor")
                    armor_val = 0
                    if armor_name:
                        armor_item = get_item_by_name(armor_name)
                        if armor_item:
                            armor_val = int(armor_item.get("armor", 0) or 0)

                    effective = max(0, dmg - armor_val)
                    new_hp = max(0, char.get("hp", 0) - effective)
                    c = conn()
                    cur = c.cursor()
                    cur.execute("UPDATE characters SET hp = ? WHERE user_id = ?", (new_hp, target_id))
                    c.commit()
                    c.close()
                    await message.answer(f"Игрок {char['username']} получил {dmg} урона (броня {armor_val} уменьшила урон до {effective}). Текущее HP: {new_hp}", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    GM_SESSIONS.pop(user_id, None)
                    return
                if gstep == "gm_input_heal":
                    if message.chat.type != "private":
                        return
                    if text == "Отмена":
                        GM_SESSIONS.pop(user_id, None)
                        await message.answer("Отменено.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        return
                    try:
                        heal = int(text)
                    except ValueError:
                        await message.answer("Введите целое число лечения.")
                        return
                    target_id = gs.get("target_id")
                    char = load_character_full(target_id)
                    if not char:
                        await message.answer("Игрок не найден.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        return
                    strength = char["attrs"].get("сила",0)
                    race_bonus_strength = int(RACE_BONUSES.get(char.get('race'), {}).get("сила", 0) or 0)
                    max_hp = round((strength + race_bonus_strength) * 2.2)
                    # max_hp = round(char["attrs"].get("сила",0)*2.2)
                    logger.info("heal=%s| Hp=%s",heal,char.get("hp",0))
                    new_hp = min(max_hp, char.get("hp",0) + heal)
                    logger.info(new_hp)
                    c = conn()
                    cur = c.cursor()
                    cur.execute("UPDATE characters SET hp = ? WHERE user_id = ?", (new_hp, target_id))
                    c.commit()
                    c.close()
                    await message.answer(f"Игрок {char['username']} восстановил {heal} HP. Текущее HP: {new_hp}", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    GM_SESSIONS.pop(user_id, None)
                    return
                if gstep == "gm_trade_choose":
                    if message.chat.type != "private":
                        return
                    sel = text
                    trade_map = gs.get("trade_map", {})
                    if sel not in trade_map:
                        await message.answer("Неверный выбор.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    item = trade_map[sel]
                    target_id = gs.get("target_id")
                    char = load_character_full(target_id)
                    if not char:
                        await message.answer("Игрок не найден.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    # admin buys item for player: check admin gold? we assume admin has infinite funds; per spec we deduct player's gold
                    cost = item["cost"]
                    if char.get("gold",0) < cost:
                        await message.answer(f"У игрока недостаточно золота ({char.get('gold',0)}g). Товар стоит {cost}g.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                        GM_SESSIONS.pop(user_id, None)
                        return
                    # deduct gold and add item to inventory
                    inv_ids = char.get("inventory_ids", [])[:]
                    inv_ids.append(item["id"])  # добавляем ID предмета

                    new_gold = char.get("gold", 0) - cost

                    c = conn()
                    cur = c.cursor()
                    cur.execute("""
                        UPDATE characters
                        SET inventory = ?, gold = ?
                        WHERE user_id = ?
                    """, (
                        json.dumps(inv_ids, ensure_ascii=False),
                        new_gold,
                        target_id
                    ))
                    c.commit()
                    c.close()
                    await message.answer(f"Товар {item['name']} продан игроку {char['username']}. Осталосb золота: {new_gold}", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    GM_SESSIONS.pop(user_id, None)
                    return

            if text == "Магазины":
                if message.chat.type != "private":
                    return
                # list stores and mark active
                c = conn()
                cur = c.cursor()
                cur.execute("SELECT id, name, active FROM stores")
                rows = cur.fetchall()
                c.close()
                labels = [f"{r[1]} {'(активен)' if r[2]==1 else ''}".strip() for r in rows]
                mapping = {labels[i]: rows[i][0] for i in range(len(rows))}
                GM_SESSIONS[user_id] = {"step": "choose_store", "store_map": mapping}
                await message.answer("Выберите магазин (активный переключится):", reply_markup=make_keyboard_from_options(labels, cols=1))
                return
            if user_id in GM_SESSIONS and GM_SESSIONS[user_id].get("step") == "choose_store":
                if message.chat.type != "private":
                    return
                sel = (message.text or "").strip()
                store_map = GM_SESSIONS[user_id].get("store_map", {})
                if sel not in store_map:
                    await message.answer("Неверный выбор.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                    GM_SESSIONS.pop(user_id, None)
                    return
                sid = store_map[sel]
                set_active_store(sid)
                await message.answer("Магазин переключён.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
                GM_SESSIONS.pop(user_id, None)
                return
        # Regular chat commands for non-sessions

    except Exception:
        logger.exception("Exception in universal_handler")
        try:
            await message.answer("Внутренняя ошибка. Проверьте логи.")
        except Exception:
            logger.exception("Failed to send error message to user")

# # ====== Stores admin handler (manage stores) via button "Магазины" in main menu for admin ======
# @dp.message()
# async def stores_admin_handler(message: Message):
#     # this handler overlaps universal; check only admin in group pressing "Магазины" or selecting store
#     try:
#         if message.entities:
#             for ent in message.entities:
#                 if ent.type == "bot_command":
#                     return
#         user_id = message.from_user.id
#         if message.chat.type in ("group","supergroup") and user_id == ADMIN_ID:
#             text = (message.text or "").strip()
#             # open stores menu
#             if text == "Магазины":
#                 # list stores and mark active
#                 c = conn()
#                 cur = c.cursor()
#                 cur.execute("SELECT id, name, active FROM stores")
#                 rows = cur.fetchall()
#                 c.close()
#                 labels = [f"{r[1]} {'(активен)' if r[2]==1 else ''}".strip() for r in rows]
#                 mapping = {labels[i]: rows[i][0] for i in range(len(rows))}
#                 GM_SESSIONS[user_id] = {"step": "choose_store", "store_map": mapping}
#                 await message.answer("Выберите магазин (активный переключится):", reply_markup=make_keyboard_from_options(labels, cols=1))
#                 return
#             if user_id in GM_SESSIONS and GM_SESSIONS[user_id].get("step") == "choose_store":
#                 sel = (message.text or "").strip()
#                 store_map = GM_SESSIONS[user_id].get("store_map", {})
#                 if sel not in store_map:
#                     await message.answer("Неверный выбор.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
#                     GM_SESSIONS.pop(user_id, None)
#                     return
#                 sid = store_map[sel]
#                 set_active_store(sid)
#                 await message.answer("Магазин переключён.", reply_markup=main_menu_keyboard(message.from_user.id,message.chat.type))
#                 GM_SESSIONS.pop(user_id, None)
#                 return
#     except Exception:
#         logger.exception("stores_admin_handler exception")

# ====== START ======
async def main():
    init_db()
    logger.info("Bot starting...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logger.info("Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())