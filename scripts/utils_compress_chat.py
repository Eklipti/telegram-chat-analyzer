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

    return " ".join(result)


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

    return "".join(result)


def clean_username(username):
    """
    Очищает ник от знаков препинания (если ник не состоит только из них).
    """
    cleaned = re.sub(r"[.,!?:;-]", "", username, flags=re.UNICODE)
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

    with open(input_file, encoding="utf-8") as f:
        lines = f.readlines()

    # Паттерн для удаления эмодзи
    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # смайлики
        "\U0001f300-\U0001f5ff"  # символы и пиктограммы
        "\U0001f680-\U0001f6ff"  # транспорт и символы карты
        "\U0001f700-\U0001f77f"  # алхимические символы
        "\U0001f780-\U0001f7ff"  # геометрические символы
        "\U0001f800-\U0001f8ff"  # стрелки
        "\U0001f900-\U0001f9ff"  # дополнительные символы
        "\U0001fa00-\U0001fa6f"  # расширенные символы
        "\U0001fa70-\U0001faff"  # символы и пиктограммы расширенные
        "\U0001f1e6-\U0001f1ff"  # флаги (региональные индикаторы)
        "\U00002702-\U000027b0"  # Dingbats
        "\U000024c2-\U0001f251"  # различные символы
        "]+",
        flags=re.UNICODE,
    )

    processed_messages = []
    current_user = None
    current_messages: list[str] = []

    for line in lines:
        line = line.strip()

        if not line:
            continue

        # Формат: username [2025-12-22 00:01:21]
        user_match = re.match(r"^(.+?)\s*\[[\d\-\s:]+]$", line)

        if user_match:
            if current_user and current_messages:
                if len(current_messages) == 1:
                    processed_messages.append(f"{current_user}: {current_messages[0]}")
                else:
                    processed_messages.append(f"{current_user}:")
                    for msg in current_messages:
                        processed_messages.append(msg)
                current_messages = []

            username = user_match.group(1)

            username = re.sub(r"\s*\([^)]*\)", "", username)
            username = username.strip()

            username = username.split()[0] if username.split() else username

            username = emoji_pattern.sub("", username).strip()

            username = clean_username(username)

            current_user = username
        else:
            message = line.strip('"')

            # Заменяем ссылки на http://*
            message = re.sub(r"https?://\S+", "http://*", message)

            message = emoji_pattern.sub("", message)

            # Оставляем: буквы, цифры, пробелы, @, /, *., ,, !, ?, :, ;
            message = re.sub(r"[^\w\s@/*.,!?:;-]", "", message, flags=re.UNICODE)

            message = remove_char_repetitions(message, max_repeats=2)

            message = limit_word_repetitions(message, max_repeats=5)

            words = message.split()
            if words and all(word.startswith("@") for word in words):
                continue

            message_stripped = message.strip()
            clean_msg = message_stripped.replace("http://*", "").strip()
            if message_stripped.startswith("http://*") and not clean_msg:
                continue

            if len(message.strip()) < min_message_length:
                continue

            if len(message) > max_message_length:
                message = message[:max_message_length].strip()

            if current_user:
                if not current_messages or current_messages[-1] != message:
                    current_messages.append(message)

    if current_user and current_messages:
        if len(current_messages) == 1:
            processed_messages.append(f"{current_user}: {current_messages[0]}")
        else:
            processed_messages.append(f"{current_user}:")
            for msg in current_messages:
                processed_messages.append(msg)

    processed_lines = processed_messages

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(processed_lines))

    return len(processed_lines)


def get_file_stats(filepath):
    """Возвращает статистику файла: размер в байтах и количество символов"""
    size_bytes = os.path.getsize(filepath)
    with open(filepath, encoding="utf-8") as f:
        content = f.read()
        char_count = len(content)
    return size_bytes, char_count
