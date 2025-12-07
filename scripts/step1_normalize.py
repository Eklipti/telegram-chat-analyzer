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
import logging
from pathlib import Path
from typing import Optional
from . import utils
from .utils import MEDIA_MAP, apply_shift_and_format 
from datetime import datetime

logger = logging.getLogger(__name__)

def normalize_json(input_path: Path, output_dir: Optional[Path]) -> Path:
    """
    Нормализует "сырой" JSON-экспорт.
    - Применяет часовой сдвиг из имени файла.
    - "Уплощает" текст в text_plain.
    - Категоризирует медиа в media_cat.
    - Складывает все новые поля в m["meta_norm"] = {...}
    """
    
    shift = utils.parse_filename_shift(input_path)
    if shift is None:
        logger.warning(f"Имя файла '{input_path.name}' не содержит часовой сдвиг в формате [number].")
        logger.warning("Принят сдвиг по умолчанию: 0 (UTC).")
        shift = 0    

    out_dir = output_dir or utils.PROCESSED_JSON_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    dst = utils.replace_shift_with_zero(out_dir / input_path.name)

    data = utils.load_json(input_path)
    msgs = data.get("messages")
    if not isinstance(msgs, list):
        raise ValueError("Ожидается messages[]")

    changed = 0

    for m in msgs:
        normalized_data = {}

        dt_naive = None
        # Используем date в приоритете, так как время в raw JSON неопределенное
        if isinstance(m.get("date"), str):
            dt_naive = utils.parse_iso_dt_naive(m.get("date"))
        if dt_naive is None and isinstance(m.get("date_unixtime"), str):
            dt_naive = utils.dt_from_unixtime_str(m.get("date_unixtime"))
        
        date_norm = apply_shift_and_format(dt_naive, shift) if dt_naive else None
        
        if date_norm:
            normalized_data["date_norm"] = date_norm
            changed += 1
        else:
            normalized_data["date_norm"] = None


        edt_naive = None
        if "edited" in m or "edited_unixtime" in m:
            if isinstance(m.get("edited_unixtime"), str):
                edt_naive = utils.dt_from_unixtime_str(m.get("edited_unixtime"))
            if edt_naive is None and isinstance(m.get("edited"), str):
                edt_naive = utils.parse_iso_dt_naive(m.get("edited"))
            if edt_naive:
                normalized_data["edited_norm"] = apply_shift_and_format(edt_naive, shift)

        normalized_data["text_plain"] = utils.flatten_text(m.get("text"))

        media_cat = None
        media_type = m.get("media_type")
        
        if media_type not in (None, ""):
            media_cat = MEDIA_MAP.get(str(media_type), "other")
        else:
            if isinstance(m.get("poll"), dict):
                media_cat = "poll"
            elif m.get("sticker_emoji"):
                media_cat = "sticker"
            elif "photo" in m:
                media_cat = "photo"
        
        normalized_data["media_cat"] = media_cat

        m["meta_norm"] = normalized_data
    if "meta" not in data or not isinstance(data.get("meta"), list):
        data["meta"] = []
    
    data["meta"].append({
        "by_normalize": {
            "applied_shift_hours": shift,
            "note": f"Созданы поля 'meta_norm' с примененным сдвигом {shift:+d}",
            "messages_with_date_norm": changed
        }
    })

    utils.save_json(dst, data)
    return dst