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
from typing import Any, Dict, List
from . import utils
from datetime import datetime
import re
from urllib.parse import urlparse

try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False

STOPWORDS_RU = {
    'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то', 'все', 'она', 'так',
    'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне', 'было',
    'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг',
    'ли', 'если', 'уже', 'или', 'ни', 'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж',
    'вам', 'ведь', 'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они', 'тут', 'где', 'есть',
    'надо', 'ней', 'для', 'мы', 'тебя', 'их', 'чем', 'была', 'сам', 'чтоб', 'без', 'будто', 'чего',
    'раз', 'тоже', 'себе', 'под', 'будет', 'ж', 'тогда', 'кто', 'этот', 'того', 'потому', 'этого',
    'какой', 'совсем', 'ним', 'здесь', 'этом', 'один', 'почти', 'мой', 'тем', 'чтобы', 'нее',
    'сейчас', 'были', 'куда', 'зачем', 'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об',
    'другой', 'хоть', 'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас', 'про', 'всего',
    'них', 'какая', 'много', 'разве', 'три', 'эту', 'моя', 'впрочем', 'хорошо', 'свою', 'этой',
    'перед', 'иногда', 'лучше', 'чуть', 'том', 'нельзя', 'такой', 'им', 'более', 'всегда', 'конечно',
    'всю', 'между', 'это', 'который', 'которая', 'которые'
}

STOPWORDS_EN = {
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i', 'it', 'for', 'not', 'on', 'with',
    'he', 'as', 'you', 'do', 'at', 'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her',
    'she', 'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what', 'so', 'up',
    'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me', 'when', 'make', 'can', 'like', 'time',
    'no', 'just', 'him', 'know', 'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could',
    'them', 'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come', 'its', 'over', 'think',
    'also', 'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first', 'well', 'way', 'even',
    'new', 'want', 'because', 'any', 'these', 'give', 'day', 'most', 'us', 'is', 'was', 'are', 'been',
    'has', 'had', 'were', 'said', 'did', 'having', 'may', 'should', 'am', 'being', 'does'
}

STOPWORDS = STOPWORDS_RU | STOPWORDS_EN

def calculate_mattr(words: List[str], window_size: int = 1000) -> float:
    """
    Вычисляет MATTR (Moving-Average Type-Token Ratio).
    Среднее разнообразие слов в скользящем окне фиксированного размера.
    """
    if len(words) < window_size:
        return 0.0
    
    ttr_values = []
    for i in range(len(words) - window_size + 1):
        window = words[i:i + window_size]
        unique_in_window = len(set(window))
        ttr = unique_in_window / window_size
        ttr_values.append(ttr)
    
    return sum(ttr_values) / len(ttr_values) if ttr_values else 0.0

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
    
    voice_duration_by_user: Dict[str, float] = defaultdict(float)
    domain_counter: Counter = Counter()
    caps_by_user: Dict[str, int] = defaultdict(int)
    formatting_by_user: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    words_by_user: Dict[str, List[str]] = defaultdict(list)
    message_count_by_user: Dict[str, int] = defaultdict(int)
    
    reply_times_by_user: Dict[str, List[int]] = defaultdict(list)
    hour_distribution_by_user: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    edit_count_by_user: Dict[str, int] = defaultdict(int)
    msg_id_to_timestamp: Dict[int, int] = {}
    
    morph = None
    if PYMORPHY_AVAILABLE:
        try:
            morph = pymorphy3.MorphAnalyzer()
            print("✓ Лемматизация включена (pymorphy3)")
        except Exception as e:
            print(f"⚠ Лемматизация недоступна (ошибка инициализации): {e}")
            print("  Анализ продолжится без лемматизации")
    else:
        print("⚠ Лемматизация недоступна (pymorphy3 не установлен)")
        print("  Анализ продолжится без лемматизации")

    for m in msgs:
        if m.get("type") != "message":
            continue
            
        mid = m.get("id")
        fid = m.get("from_id")
        
        if isinstance(mid, int) and fid:
            fid_str = str(fid)
            msg_id_to_author[mid] = fid_str
            
            timestamp = m.get("date_unixtime")
            if isinstance(timestamp, (int, str)):
                msg_id_to_timestamp[mid] = int(timestamp)
            
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
        
        reply_to_id = m.get("reply_to_message_id")
        if isinstance(reply_to_id, int) and reply_to_id in msg_id_to_author:
            replied_to_author = msg_id_to_author[reply_to_id]
            
            if fid_str != replied_to_author:
                reply_matrix[fid_str][replied_to_author] += 1
            
            quotability_counter[replied_to_author] += 1
            
            current_timestamp = m.get("date_unixtime")
            original_timestamp = msg_id_to_timestamp.get(reply_to_id)
            if isinstance(current_timestamp, (int, str)) and original_timestamp:
                reply_time = int(current_timestamp) - original_timestamp
                if reply_time > 0:
                    reply_times_by_user[fid_str].append(reply_time)
        
        message_count_by_user[fid_str] += 1
        
        date_str = m.get("date")
        if isinstance(date_str, str):
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                hour = dt.hour
                hour_distribution_by_user[fid_str][hour] += 1
            except:
                pass
        
        if m.get("edited"):
            edit_count_by_user[fid_str] += 1
        
        media_type = m.get("media_type")
        if media_type == "voice_message":
            duration = m.get("duration_seconds")
            if isinstance(duration, (int, float)) and duration > 0:
                voice_duration_by_user[fid_str] += duration
        
        if isinstance(text_entities, list):
            for entity in text_entities:
                if not isinstance(entity, dict):
                    continue
                
                entity_type = entity.get("type")
                
                if entity_type in ("link", "text_link"):
                    url = entity.get("href") or entity.get("text", "")
                    if url:
                        try:
                            parsed = urlparse(url if url.startswith(('http://', 'https://')) else 'http://' + url)
                            domain = parsed.netloc.lower()
                            if domain:
                                domain_counter[domain] += 1
                        except:
                            pass
                
                if entity_type in ("bold", "italic", "spoiler", "custom_emoji", "underline", "strikethrough"):
                    formatting_by_user[fid_str][entity_type] += 1
        
        meta_norm = m.get("meta_norm", {})
        text_plain = meta_norm.get("text_plain") or utils.flatten_text(m.get("text", ""))
        
        if text_plain:
            caps_count = sum(1 for c in text_plain if c.isupper())
            total_letters = sum(1 for c in text_plain if c.isalpha())
            if total_letters > 10 and caps_count / total_letters > 0.7:
                caps_by_user[fid_str] += 1
            
            words_raw = re.findall(r'\b[а-яёa-z]{3,}\b', text_plain.lower())
            
            for word in words_raw:
                if word in STOPWORDS:
                    continue
                
                if morph:
                    parsed = morph.parse(word)[0]
                    lemma = parsed.normal_form
                    words_by_user[fid_str].append(lemma)
                else:
                    words_by_user[fid_str].append(word)

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
    
    voice_lovers_top = []
    for user_id, total_seconds in sorted(voice_duration_by_user.items(), key=lambda x: x[1], reverse=True)[:10]:
        hours = total_seconds / 3600
        voice_lovers_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "total_seconds": round(total_seconds, 1),
            "total_hours": round(hours, 2)
        })
    
    domains_top = []
    for domain, count in domain_counter.most_common(15):
        domains_top.append({
            "domain": domain,
            "count": int(count)
        })
    
    caps_screamers_top = []
    for user_id, caps_msg_count in sorted(caps_by_user.items(), key=lambda x: x[1], reverse=True)[:10]:
        total_msgs = message_count_by_user.get(user_id, 1)
        caps_screamers_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "caps_messages": int(caps_msg_count),
            "total_messages": int(total_msgs),
            "caps_percentage": round(caps_msg_count / total_msgs * 100, 2) if total_msgs > 0 else 0
        })
    
    formatting_stylists_top = []
    for user_id, formatting_dict in formatting_by_user.items():
        total_formatting = sum(formatting_dict.values())
        formatting_stylists_top.append((user_id, total_formatting, formatting_dict))
    formatting_stylists_top.sort(key=lambda x: x[1], reverse=True)
    
    formatting_top = []
    for user_id, total_fmt, fmt_dict in formatting_stylists_top[:10]:
        formatting_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "total_formatting": int(total_fmt),
            "breakdown": {k: int(v) for k, v in fmt_dict.items()}
        })
    
    vocabulary_top = []
    for user_id, words_list in words_by_user.items():
        msg_count = message_count_by_user.get(user_id, 0)
        total_words = len(words_list)
        
        if msg_count >= 1000 and total_words >= 1000:
            mattr_score = calculate_mattr(words_list, window_size=1000)
            mattr_percentage = mattr_score * 100
            vocabulary_top.append((user_id, total_words, msg_count, mattr_percentage))
    
    vocabulary_top.sort(key=lambda x: x[3], reverse=True)
    
    vocabulary_top_list = []
    for user_id, total_words, msg_count, mattr_pct in vocabulary_top[:10]:
        vocabulary_top_list.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "total_words": int(total_words),
            "total_messages": int(msg_count),
            "mattr_score": round(mattr_pct, 2)
        })
    
    reaction_speed_top = []
    for user_id, reply_times in reply_times_by_user.items():
        total_msgs = message_count_by_user.get(user_id, 0)
        if total_msgs < 1000 or len(reply_times) == 0:
            continue
        
        reply_times_sorted = sorted(reply_times)
        mid_idx = len(reply_times_sorted) // 2
        if len(reply_times_sorted) % 2 == 0:
            median_seconds = (reply_times_sorted[mid_idx - 1] + reply_times_sorted[mid_idx]) / 2
        else:
            median_seconds = reply_times_sorted[mid_idx]
        
        reaction_speed_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "median_seconds": round(median_seconds, 1),
            "median_minutes": round(median_seconds / 60, 1),
            "median_hours": round(median_seconds / 3600, 2),
            "total_replies": len(reply_times),
            "total_messages": total_msgs
        })
    
    reaction_speed_top.sort(key=lambda x: x["median_seconds"])
    reaction_speed_top = reaction_speed_top[:10]
    
    owls_vs_larks = []
    for user_id, hour_dist in hour_distribution_by_user.items():
        total_msgs = sum(hour_dist.values())
        if total_msgs < 1000:
            continue
        
        night_hours = sum(hour_dist.get(h, 0) for h in range(1, 7))
        day_hours = sum(hour_dist.get(h, 0) for h in range(9, 18))
        
        night_pct = (night_hours / total_msgs * 100) if total_msgs > 0 else 0
        day_pct = (day_hours / total_msgs * 100) if total_msgs > 0 else 0
        
        if night_pct > 30:
            category = "Сова"
        elif day_pct > 50:
            category = "Жаворонок"
        else:
            category = "Нейтральный"
        
        owls_vs_larks.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "category": category,
            "night_percentage": round(night_pct, 2),
            "day_percentage": round(day_pct, 2),
            "total_messages": total_msgs
        })
    
    owls_vs_larks.sort(key=lambda x: x["night_percentage"], reverse=True)
    
    self_censorship_top = []
    for user_id, edit_count in edit_count_by_user.items():
        total_msgs = message_count_by_user.get(user_id, 0)
        if total_msgs < 1000:
            continue
        
        edit_pct = (edit_count / total_msgs * 100) if total_msgs > 0 else 0
        self_censorship_top.append({
            "user_id": user_id,
            "username": name_by_id.get(user_id, user_id),
            "edited_messages": int(edit_count),
            "total_messages": int(total_msgs),
            "edit_percentage": round(edit_pct, 2)
        })
    
    self_censorship_top.sort(key=lambda x: x["edit_percentage"], reverse=True)
    self_censorship_top = self_censorship_top[:10]
    
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
        },
        
        "voice_lovers": {
            "description": "Топ пользователей по суммарной длительности голосовых сообщений",
            "top_users": voice_lovers_top
        },
        
        "external_links": {
            "description": "Какие домены чаще всего упоминаются в чате",
            "top_domains": domains_top
        },
        
        "caps_screamers": {
            "description": "Кто злоупотребляет CAPS LOCK",
            "top_users": caps_screamers_top
        },
        
        "formatting_stylists": {
            "description": "Кто чаще использует форматирование текста",
            "top_users": formatting_top
        },
        
        "vocabulary_diversity": {
            "description": "Лексическое разнообразие по методу MATTR (Moving-Average Type-Token Ratio). Среднее разнообразие в скользящем окне 1000 слов. Учитывается лемматизация и фильтрация стоп-слов (минимум 1000 сообщений и 1000 слов)",
            "top_users": vocabulary_top_list
        },
        
        "reaction_speed": {
            "description": "Медианное время ответа на сообщения через reply для каждого пользователя (топ-10 самых быстрых, минимум 1000 сообщений)",
            "top_users": reaction_speed_top
        },
        
        "owls_vs_larks": {
            "description": "Категоризация пользователей по времени активности. Совы (ночь 01:00-06:00 > 30%), Жаворонки (день 09:00-17:00 > 50%), минимум 1000 сообщений",
            "users": owls_vs_larks
        },
        
        "self_censorship": {
            "description": "Индекс самоцензуры - кто чаще редактирует свои сообщения после отправки (топ-10, минимум 1000 сообщений)",
            "top_users": self_censorship_top
        }
    }
    
    # Сохраняем результат
    out_file = out_dir / "social_graph.json"
    utils.save_json(out_file, social_graph_data)
    
    print(f"✓ Социальный граф сохранен: {out_file}")
    print(f"  - Всего упоминаний: {total_mentions}")
    print(f"  - Всего ответов: {total_replies}")
    print(f"  - Уникальных цитируемых: {len(quotability_counter)}")
