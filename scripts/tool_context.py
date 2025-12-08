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
from datetime import datetime, timedelta
from typing import Optional, Tuple
from . import utils

logger = logging.getLogger(__name__)

def parse_date_argument(date_arg: str) -> Tuple[datetime, datetime]:
    """
    Парсит аргумент --date и возвращает кортеж (start_date, end_date).
    
    Поддерживаемые форматы:
    - `-1` - вчерашний день
    - `{YYYY-MM-DD}` - один день
    - `{YYYY-MM-DD}_{YYYY-MM-DD}` - период
    """
    if date_arg == "-1":
        # Вчерашний день
        yesterday = datetime.now().date() - timedelta(days=1)
        start_date = datetime.combine(yesterday, datetime.min.time())
        end_date = datetime.combine(yesterday, datetime.max.time())
        return start_date, end_date
    
    if "_" in date_arg:
        # Период: {YYYY-MM-DD}_{YYYY-MM-DD}
        parts = date_arg.split("_", 1)
        if len(parts) != 2:
            raise ValueError(f"Неверный формат периода: {date_arg}. Ожидается YYYY-MM-DD_YYYY-MM-DD")
        
        try:
            start_date = datetime.strptime(parts[0], "%Y-%m-%d")
            end_date = datetime.strptime(parts[1], "%Y-%m-%d")
            # Устанавливаем время для end_date на конец дня
            end_date = datetime.combine(end_date.date(), datetime.max.time())
        except ValueError as e:
            raise ValueError(f"Неверный формат даты: {date_arg}. Ожидается YYYY-MM-DD") from e
        
        if start_date > end_date:
            raise ValueError(f"Начальная дата не может быть позже конечной: {date_arg}")
        
        return start_date, end_date
    else:
        # Один день: {YYYY-MM-DD}
        try:
            date_obj = datetime.strptime(date_arg, "%Y-%m-%d")
            start_date = datetime.combine(date_obj.date(), datetime.min.time())
            end_date = datetime.combine(date_obj.date(), datetime.max.time())
            return start_date, end_date
        except ValueError as e:
            raise ValueError(f"Неверный формат даты: {date_arg}. Ожидается YYYY-MM-DD или YYYY-MM-DD_YYYY-MM-DD") from e

def extract_date_from_norm(date_norm: Optional[str]) -> Optional[datetime]:
    """
    Извлекает дату из поля date_norm (формат ISO с временной зоной).
    Возвращает naive datetime для сравнения.
    """
    if not date_norm:
        return None
    
    try:
        # Парсим ISO формат с временной зоной
        dt = datetime.fromisoformat(date_norm.replace("Z", "+00:00"))
        # Возвращаем naive datetime для сравнения
        return dt.replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None

def format_date_for_output(date_norm: Optional[str]) -> str:
    """
    Форматирует date_norm для вывода в формате [YYYY-MM-DD HH:MM:SS].
    """
    if not date_norm:
        return "[неизвестная дата]"
    
    try:
        dt = datetime.fromisoformat(date_norm.replace("Z", "+00:00"))
        # Убираем временную зону для вывода
        dt_naive = dt.replace(tzinfo=None)
        return dt_naive.strftime("[%Y-%m-%d %H:%M:%S]")
    except (ValueError, AttributeError):
        return "[неизвестная дата]"

def generate_context_report(input_path: Path, output_path: Path, date_arg: str) -> None:
    """
    Генерирует текстовый отчет с историей сообщений за указанный период.
    Использует нормализованный JSON ([0].json).
    """
    data = utils.load_json(input_path)
    msgs = data.get("messages", [])
    chat_id = data.get("id", "unknown_chat_id")
    
    # Парсим аргумент даты
    try:
        start_date, end_date = parse_date_argument(date_arg)
        logger.info(f"Фильтрация сообщений за период: {start_date.date()} - {end_date.date()}")
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты: {e}")
        raise
    
    # Фильтруем и собираем сообщения
    filtered_messages = []
    
    for m in msgs:
        meta = m.get("meta_norm")
        if not meta:
            continue
        
        date_norm = meta.get("date_norm")
        msg_date = extract_date_from_norm(date_norm)
        
        if msg_date is None:
            continue
        
        # Проверяем, попадает ли сообщение в период
        if start_date <= msg_date <= end_date:
            from_name = m.get("from", "Unknown")
            text_plain = meta.get("text_plain", "")
            
            if not text_plain or not text_plain.strip():
                continue
            
            filtered_messages.append({
                "from": from_name,
                "date_norm": date_norm,
                "text_plain": text_plain,
                "msg_date": msg_date
            })
    
    # Сортируем по дате (хронологический порядок)
    filtered_messages.sort(key=lambda x: x["msg_date"])
    
    logger.info(f"Найдено {len(filtered_messages)} сообщений за указанный период")
    
    if not filtered_messages:
        logger.warning("Не найдено сообщений за указанный период")
    
    tool_output_dir = utils.OUT_DIR / "context"
    tool_output_dir.mkdir(parents=True, exist_ok=True)
    
    if date_arg == "-1":
        date_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif "_" in date_arg:
        date_str = date_arg.replace("_", "_to_")
    else:
        date_str = date_arg
    
    txt_path = tool_output_dir / f"context_{date_str}.txt"
    
    try:
        with open(txt_path, "w", encoding="utf-8") as f:
            if not filtered_messages:
                return
            
            # Группируем сообщения от одного пользователя, идущие подряд
            # Считаем "стеной" сообщения от одного пользователя с интервалом < 5 минут
            WALL_THRESHOLD = timedelta(minutes=5)
            
            current_group = None
            
            for msg in filtered_messages:
                from_name = msg["from"]
                date_formatted = format_date_for_output(msg["date_norm"])
                text_plain = msg["text_plain"]
                msg_date = msg["msg_date"]
                
                # Проверяем, можно ли добавить к текущей группе
                if current_group is not None:
                    last_msg_date = current_group["last_date"]
                    time_diff = msg_date - last_msg_date
                    
                    # Если тот же пользователь и разница < порога - добавляем в группу
                    if (current_group["from"] == from_name and 
                        time_diff < WALL_THRESHOLD):
                        current_group["texts"].append(text_plain)
                        current_group["last_date"] = msg_date
                        continue
                    else:
                        # Завершаем текущую группу и выводим
                        f.write(f"{current_group['from']} {current_group['date']}\n")
                        for text in current_group["texts"]:
                            f.write(f'"{text}"\n')
                        f.write("\n")
                
                # Начинаем новую группу
                current_group = {
                    "from": from_name,
                    "date": date_formatted,
                    "texts": [text_plain],
                    "last_date": msg_date
                }
            
            # Выводим последнюю группу
            if current_group is not None:
                f.write(f"{current_group['from']} {current_group['date']}\n")
                for text in current_group["texts"]:
                    f.write(f'"{text}"\n\n')
        
        logger.info(f"Контекстный отчет сохранен: {txt_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при создании TXT файла: {e}")
        raise
