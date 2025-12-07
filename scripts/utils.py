# Telegram Chat Analytics Pipeline
# Copyright (C) 2025 Eklipti
#
# Этот проект — свободное программное обеспечение: вы можете
# распространять и/или изменять его на условиях
# Стандартной общественной лицензии GNU (GNU GPL)
# третьей версии, опубликованной Фондом свободного ПО.
#
# Программа распространяется в надежде, что она будет полезной,
# но БЕЗ КАКИХ-ЛИБО ГАРАНТИЙ; даже без подразумеваемой гарантии
# ТОВАРНОГО СОСТОЯНИЯ или ПРИГОДНОСТИ ДЛЯ КОНКРЕТНОЙ ЦЕЛИ.
# Подробности см. в Стандартной общественной лицензии GNU.
#
# Вы должны были получить копию Стандартной общественной
# лицензии GNU вместе с этой программой. Если это не так,
# см. <https://www.gnu.org/licenses/>.

from __future__ import annotations

import re
import json
import hashlib

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional, List, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BASE_EXPORT_DIR = PROJECT_ROOT / "telegram" / "exports"
RAW_JSON_DIR = BASE_EXPORT_DIR / "raw_json"
PROCESSED_JSON_DIR = BASE_EXPORT_DIR / "processed_json"

OUT_DIR = PROJECT_ROOT / "output"
MD_DIR  = OUT_DIR / "md"
AGG_DIR = OUT_DIR / "agg"

FILENAME_RE = re.compile(
    r"""^(?P<prefix>.*)           
        \[(?P<shift>[+-]?\d+)\]   
        \.json$                  
    """,
    re.VERBOSE | re.IGNORECASE
)

MEDIA_MAP: Dict[str, str] = {
    "photo": "photo",
    "video": "video",
    "video_file": "video",
    "audio_file": "audio_file",
    "voice_message": "voice_message",
    "video_message": "video_message",
    "sticker": "sticker",
    "animation": "animation (GIF)",
    "gif": "animation (GIF)",
    "document": "document",
    "file": "document",
    "poll": "poll",
    "contact": "contact",
    "location": "location",
    "game": "game",
}

MEDIA_CATEGORIES_ORDER: List[str] = [
    "photo", "video", "audio_file", "voice_message", "video_message",
    "sticker", "animation (GIF)", "document", "poll", "contact",
    "location", "game", "other",
]


def _norm_time_fragment(t: str) -> Optional[str]:
    digits = re.sub(r"\D", "", t)
    if len(digits) != 6:
        return None
    return f"{digits[0:2]}:{digits[2:4]}:{digits[4:6]}"

def parse_filename_shift(p: Path) -> Optional[int]:
    m = FILENAME_RE.search(p.name)
    if not m: return None
    try: return int(m.group("shift"))
    except: return None

def replace_shift_with_zero(p: Path) -> Path:
    m = FILENAME_RE.search(p.name)
    if not m:
        return p.with_name(p.stem + "[0]" + p.suffix)
    
    return p.with_name(
        f'{m.group("prefix")}[0].json'
    )
def find_input_json(explicit: Optional[Path]) -> Path:
    """Ищет самый новый .json в /telegram/exports/raw_json/"""
    if explicit:
        if not explicit.exists(): raise FileNotFoundError(explicit)
        return explicit
    
    RAW_JSON_DIR.mkdir(parents=True, exist_ok=True) 
    
    cands = sorted(RAW_JSON_DIR.glob("*.json"))
    if not cands:
        raise FileNotFoundError(
            f"Не найден *.json в {RAW_JSON_DIR}. "
            "Пожалуйста, поместите 'сырой' экспорт Telegram в эту папку."
        )
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]

def find_normalized_json(explicit: Optional[Path]) -> Path:
    """ИДет самый новый [0].json в /telegram/exports/processed_json/."""
    if explicit:
        if not explicit.exists(): raise FileNotFoundError(explicit)
        return explicit

    PROCESSED_JSON_DIR.mkdir(parents=True, exist_ok=True) 
    
    cands = [p for p in PROCESSED_JSON_DIR.glob("*[0].json")]
    if not cands:
        raise FileNotFoundError(
            f"Не найден нормализованный файл ([0].json) в {PROCESSED_JSON_DIR}. "
            "Запустите 'step1_normalize' (или 'all') для его создания."
        )
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]

def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def dt_from_unixtime_str(s: str) -> Optional[datetime]:
    if not s: return None
    try: 
        dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
        return dt.replace(tzinfo=None)
    except: return None

def parse_iso_dt_naive(s: str) -> Optional[datetime]:
    if not s: return None
    try:
        dt_str = s.replace("Z", "")
        if len(dt_str) > 6 and dt_str[-6] in "+-":
            dt_str = dt_str[:-6]
        dt = datetime.fromisoformat(dt_str)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except: return None

def apply_shift_and_format(dt_naive: Optional[datetime], shift_hours: int) -> Optional[str]:
    """
    Применяет часовой сдвиг к naive datetime и форматирует с указанием сдвига.
    Время в raw JSON неопределенное, сдвиг применяется напрямую.
    """
    if dt_naive is None: 
        return None
    

    shifted_dt = dt_naive + timedelta(hours=shift_hours)

    target_tz = timezone(timedelta(hours=shift_hours))
    dt_with_tz = shifted_dt.replace(tzinfo=target_tz)
    return dt_with_tz.isoformat(timespec="seconds")

def flatten_text(text_field: Any) -> str:
    if isinstance(text_field, str): return text_field
    if isinstance(text_field, list):
        parts = []
        for seg in text_field:
            if isinstance(seg, str):
                parts.append(seg)
            elif isinstance(seg, dict):
                t = seg.get("text")
                if isinstance(t, str):
                    parts.append(t)
        return "".join(parts)
    return ""

def sha256_prefix(value: Any, length: int) -> str:
    if value is None:
        value = ""
    s = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return s[:length]