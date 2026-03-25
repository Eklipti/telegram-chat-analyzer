import os
import re


def limit_word_repetitions(text, max_repeats=5):
    """
    Ограничивает количество повторений одного слова в тексте.
    Если слово повторяется больше max_repeats раз подряд, оставляет только max_repeats.
    """
    words = text.split()
    if not words:
        return text
    
    result = []
    i = 0
    while i < len(words):
        current_word = words[i]
        count = 1
        
        while i + count < len(words) and words[i + count] == current_word:
            count += 1
        
        result.extend([current_word] * min(count, max_repeats))
        i += count
    
    return ' '.join(result)

def remove_char_repetitions(text, max_repeats=2):
    """
    Удаляет повторяющиеся символы, оставляя максимум max_repeats повторений.
    Например: 'ахаххххааа' -> 'ахахаа', '???????' -> '??'
    """
    if not text:
        return text
    
    result = []
    i = 0
    while i < len(text):
        current_char = text[i]
        count = 1
        
        while i + count < len(text) and text[i + count] == current_char:
            count += 1
        
        result.append(current_char * min(count, max_repeats))
        i += count
    
    return ''.join(result)

def clean_username(username):
    """
    Очищает ник от знаков препинания (если ник не состоит только из них).
    """
    cleaned = re.sub(r'[.,!?:;-]', '', username, flags=re.UNICODE)
    cleaned = cleaned.strip()
    
    if not cleaned:
        return username
    
    return cleaned

def process_chat_log(input_file, output_file, min_message_length=5, max_message_length=250):
    """
    Обрабатывает лог чата (формат context) согласно правилам:
    1. Удаляет пустые строки
    2. Удаляет короткие сообщения (меньше min_message_length символов)
    3. Заменяет ссылки на http://*
    4. Удаляет сообщения, состоящие только из упоминаний
    5. Удаляет все эмодзи
    6. Удаляет дубликаты последовательных сообщений
    7. Ограничивает повторения слов (максимум 5 раз)
    8. Удаляет повторяющиеся символы (максимум 2 повторения)
    9. Удаляет сообщения, состоящие только из ссылок
    10. Обрезает длинные сообщения (max_message_length символов)
    11. Сохраняет только первое слово ника
    12. Удаляет знаки препинания и эмодзи из ников
    """
    
    with open(input_file, encoding='utf-8') as f:
        lines = f.readlines()
    
    # Паттерн для удаления эмодзи
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # смайлики
        "\U0001F300-\U0001F5FF"  # символы и пиктограммы
        "\U0001F680-\U0001F6FF"  # транспорт и символы карты
        "\U0001F700-\U0001F77F"  # алхимические символы
        "\U0001F780-\U0001F7FF"  # геометрические символы
        "\U0001F800-\U0001F8FF"  # стрелки
        "\U0001F900-\U0001F9FF"  # дополнительные символы
        "\U0001FA00-\U0001FA6F"  # расширенные символы
        "\U0001FA70-\U0001FAFF"  # символы и пиктограммы расширенные
        "\U0001F1E6-\U0001F1FF"  # флаги (региональные индикаторы)
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"  # различные символы
        "]+", flags=re.UNICODE)
    
    processed_messages = []
    current_user = None
    current_messages = []
    
    for line in lines:
        line = line.strip()
        
        if not line:
            continue
        
        # Проверяем, это строка с именем пользователя и временной меткой
        # Формат: username [2025-12-22 00:01:21]
        user_match = re.match(r'^(.+?)\s*\[[\d\-\s:]+\]$', line)
        
        if user_match:
            # Это строка с ником
            if current_user and current_messages:
                if len(current_messages) == 1:
                    processed_messages.append(f"{current_user}: {current_messages[0]}")
                else:
                    processed_messages.append(f"{current_user}:")
                    for msg in current_messages:
                        processed_messages.append(msg)
                current_messages = []
            
            username = user_match.group(1)
            
            # Убираем всё, что в круглых скобках из ника
            username = re.sub(r'\s*\([^)]*\)', '', username)
            username = username.strip()
            
            # Оставляем только первое слово ника
            username = username.split()[0] if username.split() else username
            
            # Удаляем эмодзи из ника
            username = emoji_pattern.sub('', username).strip()
            
            # Очищаем от знаков препинания
            username = clean_username(username)
            
            current_user = username
        else:
            # Это строка с сообщением
            # Убираем кавычки в начале и конце
            message = line.strip('"')
            
            # Заменяем ссылки на http://*
            message = re.sub(r'https?://[^\s]+', 'http://*', message)
            
            # Удаляем эмодзи из сообщения
            message = emoji_pattern.sub('', message)
            
            # Удаляем все специальные символы кроме разрешённых знаков препинания
            # Оставляем: буквы, цифры, пробелы, @, /, *, ., ,, !, ?, :, ;
            message = re.sub(r'[^\w\s@/*.,!?:;-]', '', message, flags=re.UNICODE)
            
            # Удаляем повторяющиеся символы (максимум 2 повторения)
            message = remove_char_repetitions(message, max_repeats=2)
            
            # Ограничиваем повторения слов (максимум 5 раз по умолчанию)
            message = limit_word_repetitions(message, max_repeats=5)
            
            # Проверяем, состоит ли сообщение только из упоминаний (@username)
            words = message.split()
            if words and all(word.startswith('@') for word in words):
                continue
            
            # Удаляем сообщения, состоящие только из ссылок
            message_stripped = message.strip()
            if message_stripped == 'http://*' or (message_stripped.startswith('http://*') and len(message_stripped.replace('http://*', '').strip()) == 0):
                continue
            
            # Проверяем длину сообщения (без учета пробелов по краям)
            if len(message.strip()) < min_message_length:
                continue
            
            # Обрезаем длинные сообщения
            if len(message) > max_message_length:
                message = message[:max_message_length].strip()
            
            # Добавляем сообщение в список текущего пользователя (только если не дубликат)
            if current_user:
                if not current_messages or current_messages[-1] != message:
                    current_messages.append(message)
    
    # Сохраняем последние сообщения
    if current_user and current_messages:
        if len(current_messages) == 1:
            processed_messages.append(f"{current_user}: {current_messages[0]}")
        else:
            processed_messages.append(f"{current_user}:")
            for msg in current_messages:
                processed_messages.append(msg)
    
    processed_lines = processed_messages
    
    # Записываем результат
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(processed_lines))
    
    return len(processed_lines)

def get_file_stats(filepath):
    """Возвращает статистику файла: размер в байтах и количество символов"""
    size_bytes = os.path.getsize(filepath)
    with open(filepath, encoding='utf-8') as f:
        content = f.read()
        char_count = len(content)
    return size_bytes, char_count


