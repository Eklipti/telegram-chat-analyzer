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
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict
from . import utils
from datetime import datetime

def build_social_graph(input_json: Path, out_dir: Path) -> None:
    """
    Строит социальный граф и углубленную статистику взаимодействий:
    - Матрица упоминаний (@username)
    - Карта симпатий - кто кому отвечает
    - Индекс цитируемости - на чьи сообщения чаще отвечают
    """
    data = utils.load_json(input_json)
    msgs = data.get("messages") or []
    chat_id = data.get("id", "unknown_chat_id")
    out_dir.mkdir(parents=True, exist_ok=True)

    name_by_id: Dict[str, str] = {}
    mention_counter: Counter = Counter()
    reply_matrix: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    quotability_counter: Counter = Counter()
    msg_id_to_author: Dict[int, str] = {}

    for m in msgs:
        if m.get("type") != "message":
            continue
            
        mid = m.get("id")
        fid = m.get("from_id")
        
        if isinstance(mid, int) and fid:
            fid_str = str(fid)
            msg_id_to_author[mid] = fid_str
            
            disp = m.get("from")
            if isinstance(disp, str) and disp.strip():
                name_by_id[fid_str] = disp

    for m in msgs:
        if m.get("type") != "message":
            continue
            
        mid = m.get("id")
        fid = m.get("from_id")
        
        if not fid:
            continue
            
        fid_str = str(fid)
        
        text_entities = m.get("text_entities")
        if isinstance(text_entities, list):
            for entity in text_entities:
                if not isinstance(entity, dict):
                    continue
                    
                entity_type = entity.get("type")
                
                if entity_type == "mention":
                    mention_text = entity.get("text", "")
                    if mention_text:
                        mention_counter[mention_text] += 1
                
                elif entity_type == "text_mention":
                    mentioned_user_id = entity.get("user_id")
                    if mentioned_user_id:
                        mentioned_id_str = str(mentioned_user_id)
                        mention_counter[mentioned_id_str] += 1
                        
                        mention_text = entity.get("text")
                        if mention_text and mentioned_id_str not in name_by_id:
                            name_by_id[mentioned_id_str] = mention_text
        
        # Анализ ответов (reply_to_message_id)
        reply_to_id = m.get("reply_to_message_id")
        if isinstance(reply_to_id, int) and reply_to_id in msg_id_to_author:
            replied_to_author = msg_id_to_author[reply_to_id]
            
            if fid_str != replied_to_author:
                reply_matrix[fid_str][replied_to_author] += 1
            
            quotability_counter[replied_to_author] += 1

    # Формируем топы для матрицы упоминаний
    mentions_top = []
    for mentioned, count in mention_counter.most_common(15):
        username = name_by_id.get(mentioned, mentioned)
        mentions_top.append({
            "mentioned": mentioned,
            "username": username,
            "count": int(count)
        })
    
    # Формируем карту симпатий (топ-10 пар)
    reply_pairs = []
    for from_user, to_users in reply_matrix.items():
        for to_user, count in to_users.items():
            reply_pairs.append((from_user, to_user, count))
    
    reply_pairs.sort(key=lambda x: x[2], reverse=True)
    
    reply_matrix_top = []
    for from_user, to_user, count in reply_pairs[:20]:
        reply_matrix_top.append({
            "from_id": from_user,
            "from_username": name_by_id.get(from_user, from_user),
            "to_id": to_user,
            "to_username": name_by_id.get(to_user, to_user),
            "count": int(count)
        })
    
    # Формируем индекс цитируемости (топ-10 самых цитируемых)
    quotability_top = []
    for user_id, count in quotability_counter.most_common(10):
        quotability_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "replies_received": int(count)
        })
    
    # Дополнительная статистика
    total_mentions = sum(mention_counter.values())
    total_replies = sum(sum(to_dict.values()) for to_dict in reply_matrix.values())
    unique_mentioners = len(set(uid for uid in mention_counter.keys()))
    unique_repliers = len(reply_matrix)
    
    # Финальная структура данных
    social_graph_data = {
        "chat_id": chat_id,
        "source_file_path": str(input_json.resolve()),
        "source_file_name": input_json.name,
        "generation_timestamp": int(datetime.now().timestamp()),
        
        "summary": {
            "total_mentions": total_mentions,
            "unique_mentioned_users": unique_mentioners,
            "total_replies": total_replies,
            "unique_repliers": unique_repliers,
            "unique_quoted_users": len(quotability_counter)
        },
        
        "mention_matrix": {
            "description": "Кого чаще всего упоминают (@username или text_mention)",
            "top_mentioned": mentions_top
        },
        
        "reply_matrix": {
            "description": "Кто кому чаще отвечает (таблица пар)",
            "top_pairs": reply_matrix_top
        },
        
        "quotability_index": {
            "description": "На чьи сообщения чаще всего отвечают",
            "top_quoted": quotability_top
        }
    }
    
    # Сохраняем результат
    out_file = out_dir / "social_graph.json"
    utils.save_json(out_file, social_graph_data)
    
    print(f"✓ Социальный граф сохранен: {out_file}")
    print(f"  - Всего упоминаний: {total_mentions}")
    print(f"  - Всего ответов: {total_replies}")
    print(f"  - Уникальных цитируемых: {len(quotability_counter)}")
