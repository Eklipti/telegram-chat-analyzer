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
from collections import defaultdict
from datetime import datetime
from . import utils

logger = logging.getLogger(__name__)

def generate_author_text_report(input_path: Path, output_path: Path) -> None:
    """
    Генерирует два отчета (JSON и TXT): Авторы -> Их сообщения.
    Использует нормализованный JSON ([0].json).
    """
    data = utils.load_json(input_path)
    msgs = data.get("messages", [])
    chat_id = data.get("id", "unknown_chat_id")
    
    author_counts = defaultdict(int)
    author_names = {}
    messages_by_user = defaultdict(dict)

    logger.info(f"Обработка {len(msgs)} сообщений для отчета...")

    for m in msgs:
        meta = m.get("meta_norm")
        if not meta:
            continue

        from_id = m.get("from_id")
        if not from_id:
            continue

        from_id_str = str(from_id)
        if from_id_str not in author_names:
            author_names[from_id_str] = m.get("from", f"Unknown ({from_id_str})")
        
        author_counts[from_id_str] += 1

        msg_id_str = str(m.get("id"))
        
        msg_obj = {
            "id": m.get("id"),
            "date": meta.get("date_norm"),
            "text": meta.get("text_plain")
        }

        messages_by_user[from_id_str][msg_id_str] = msg_obj

    # Сортируем: от большего количества сообщений к меньшему
    sorted_authors = sorted(author_counts.items(), key=lambda item: item[1], reverse=True)

    top_authors_json = {}
    
    for from_id_str, count in sorted_authors:
        name = author_names.get(from_id_str, "Unknown")
        
        original_name = name
        while name in top_authors_json:
             name = f"{original_name} ({from_id_str[:8]})"
        
        top_authors_json[name] = {
            "id": from_id_str,
            "count_message": count
        }

    final_report = {
        "chat_id": chat_id,
        "source_file_path": str(input_path.resolve()),
        "source_file_name": input_path.name,
        "generation_timestamp": int(datetime.now().timestamp()),
        "top_authors": top_authors_json
    }

    for from_id_str, msgs_dict in messages_by_user.items():
        final_report[from_id_str] = msgs_dict

    tool_output_dir = utils.OUT_DIR / "author_text"
    tool_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = tool_output_dir / output_path.name
    
    utils.save_json(output_path, final_report)
    logger.info(f"JSON-отчет сохранен: {output_path}")

    txt_path = output_path.with_suffix(".txt")
    
    try:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("ТОП ПОЛЬЗОВАТЕЛЕЙ ПО СООБЩЕНИЯМ\n")
            
            for i, (from_id_str, count) in enumerate(sorted_authors, 1):
                name = author_names.get(from_id_str, "Unknown")
                f.write(f"{i}. {name} ({count})\n")

            f.write("\n=== ВСЕ СООБЩЕНИЯ ВСЕХ ===\n")

            for i, (from_id_str, count) in enumerate(sorted_authors, 1):
                name = author_names.get(from_id_str, "Unknown")
                
                f.write(f"{i}. {name} ({count})\n")
                
                user_msgs_dict = messages_by_user.get(from_id_str, {})
                sorted_msg_ids = sorted(user_msgs_dict.keys(), key=lambda x: int(x))
                
                for mid in sorted_msg_ids:
                    text_content = user_msgs_dict[mid].get("text", "")
                    f.write(f'"{text_content}"\n')
                
                f.write("\n\n") 

        logger.info(f"TXT-отчет сохранен: {txt_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при создании TXT файла: {e}")