from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from . import utils
from .utils_compress_chat import get_file_stats, process_chat_log

logger = logging.getLogger(__name__)


def _write_context_file(txt_path: Path, filtered_messages: list) -> None:
    """
    Записывает отфильтрованные сообщения в текстовый файл.

    Args:
        txt_path: Путь к выходному файлу
        filtered_messages: Список отфильтрованных сообщений
    """
    if not filtered_messages:
        return

    WALL_THRESHOLD = timedelta(minutes=5)

    with Path(txt_path).open("w", encoding="utf-8") as f:
        current_group = None

        for msg in filtered_messages:
            from_name = msg["from"]
            date_formatted = format_date_for_output(msg["date_norm"])
            text_plain = msg["text_plain"]
            msg_date = msg["msg_date"]

            if current_group is not None:
                last_msg_date = current_group["last_date"]
                time_diff = msg_date - last_msg_date

                if current_group["from"] == from_name and time_diff < WALL_THRESHOLD:
                    current_group["texts"].append(text_plain)
                    current_group["last_date"] = msg_date
                    continue
                f.write(f"{current_group['from']} {current_group['date']}\n")
                for text in current_group["texts"]:
                    f.write(f'"{text}"\n')
                f.write("\n")

            current_group = {"from": from_name, "date": date_formatted, "texts": [text_plain], "last_date": msg_date}

        if current_group is not None:
            f.write(f"{current_group['from']} {current_group['date']}\n")
            for text in current_group["texts"]:
                f.write(f'"{text}"\n\n')


def _compress_context_file(input_path: Path, output_path: Path, min_length: int, max_length: int) -> None:
    """
    Создает сжатую версию контекстного файла.

    Args:
        input_path: Путь к исходному файлу
        output_path: Путь к сжатому файлу
        min_length: Минимальная длина сообщения
        max_length: Максимальная длина сообщения
    """
    input_size, input_chars = get_file_stats(input_path)
    process_chat_log(input_path, output_path, min_length, max_length)
    output_size, output_chars = get_file_stats(output_path)

    compression_percent = ((input_size - output_size) / input_size) * 100 if input_size > 0 else 0
    char_compres_percent = ((input_chars - output_chars) / input_chars) * 100 if input_chars > 0 else 0

    logger.debug(f"Сжатие {input_path.name}: -{compression_percent:.1f}% размер, -{char_compres_percent:.1f}% символов")


def parse_date_argument(date_arg: str) -> tuple[datetime, datetime]:
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
            end_date = datetime.combine(end_date.date(), datetime.max.time())
        except ValueError as e:
            raise ValueError(f"Неверный формат даты: {date_arg}. Ожидается YYYY-MM-DD") from e

        if start_date > end_date:
            raise ValueError(f"Начальная дата не может быть позже конечной: {date_arg}")

        return start_date, end_date
    # Один день: {YYYY-MM-DD}
    try:
        date_obj = datetime.strptime(date_arg, "%Y-%m-%d")
        start_date = datetime.combine(date_obj.date(), datetime.min.time())
        end_date = datetime.combine(date_obj.date(), datetime.max.time())
        return start_date, end_date
    except ValueError as e:
        raise ValueError(f"Неверный формат даты: {date_arg}. Ожидается YYYY-MM-DD или YYYY-MM-DD_YYYY-MM-DD") from e


def extract_date_from_norm(date_norm: str | None) -> datetime | None:
    """
    Извлекает дату из поля date_norm (формат ISO с временной зоной).
    Возвращает naive datetime для сравнения.
    """
    if not date_norm:
        return None

    try:
        dt = datetime.fromisoformat(date_norm.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def format_date_for_output(date_norm: str | None) -> str:
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


def generate_context_report(
    input_path: Path,
    output_path: Path | None,
    date_arg: str,
    compress: bool = False,
    no_save_uncompressed: bool = False,
    split_by_days: bool = False,
    max_workers: int = 2,
    batch_size: int = 10000,
    min_length: int = 5,
    max_length: int = 250,
) -> None:
    """
    Генерирует текстовый отчет с историей сообщений за указанный период.
    Использует нормализованный JSON ([0].json).

    Args:
        input_path: Путь к нормализованному JSON
        output_path: Путь для сохранения (опционально, формируется автоматически)
        date_arg: Аргумент даты (-1, YYYY-MM-DD, YYYY-MM-DD_YYYY-MM-DD)
        compress: Если True, создает дополнительно сжатую версию
        no_save_uncompressed: Если True и compress=True, не сохраняет несжатую версию
        split_by_days: Если True и date_arg содержит период, создает отдельный файл для каждого дня
        max_workers: Количество потоков для параллельной обработки в режиме split (по умолчанию 2, максимум 100)
        batch_size: Размер батча сообщений для обработки (по умолчанию 10000 строк)
        min_length: Минимальная длина сообщения для сжатой версии
        max_length: Максимальная длина сообщения для сжатой версии
    """
    if split_by_days and "_" in date_arg:
        logger.info("Режим split: создание отдельных файлов для каждого дня в периоде")

        try:
            start_date, end_date = parse_date_argument(date_arg)
        except ValueError as e:
            logger.error(f"Ошибка парсинга периода: {e}")
            raise

        # Загружаем JSON один раз для всех потоков
        logger.info(f"Загрузка данных из {input_path}")
        data = utils.load_json(input_path)
        msgs = data.get("messages", [])
        logger.info(f"Загружено {len(msgs)} сообщений")

        # Генерируем список всех дней в периоде
        current_date = start_date.date()
        end_date_only = end_date.date()
        days_list = []

        while current_date <= end_date_only:
            days_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        total_days = len(days_list)
        # Ограничиваем количество потоков: не более 100 и не более количества дней
        actual_workers = min(max_workers, total_days, 100)

        logger.info(f"Обработка {total_days} дней в {actual_workers} потоках (батчами по {batch_size} сообщений)")

        # Обрабатываем сообщения батчами для экономии памяти
        # Сначала фильтруем все сообщения по дням в батчах
        logger.info(f"Фильтрация сообщений по дням (батчами по {batch_size} сообщений)")

        # Словарь для хранения сообщений по дням
        messages_by_day = defaultdict(list)

        # Обрабатываем сообщения батчами
        total_batches = (len(msgs) + batch_size - 1) // batch_size
        for batch_idx in range(0, len(msgs), batch_size):
            batch_num = batch_idx // batch_size + 1
            msg_batch = msgs[batch_idx : batch_idx + batch_size]
            logger.info(f"Обработка батча {batch_num}/{total_batches} ({len(msg_batch)} сообщений)")

            # Фильтруем сообщения из текущего батча
            for m in msg_batch:
                meta = m.get("meta_norm")
                if not meta:
                    continue

                date_norm = meta.get("date_norm")
                msg_date = extract_date_from_norm(date_norm)

                if msg_date is None:
                    continue

                # Определяем, к какому дню относится сообщение
                msg_day_str = msg_date.strftime("%Y-%m-%d")

                # Проверяем, входит ли этот день в наш период
                if msg_day_str in days_list:
                    from_name = m.get("from", "Unknown")
                    text_plain = meta.get("text_plain", "")

                    if not text_plain or not text_plain.strip():
                        continue

                    messages_by_day[msg_day_str].append(
                        {"from": from_name, "date_norm": date_norm, "text_plain": text_plain, "msg_date": msg_date}
                    )

        logger.info("Фильтрация завершена. Найдено сообщений по дням:")
        for day_str in sorted(messages_by_day.keys()):
            logger.info(f"  {day_str}: {len(messages_by_day[day_str])} сообщений")

        # Функция для записи одного дня
        def write_single_day(date_str: str, filtered_messages: list) -> tuple[str, bool, str | None]:
            """
            Записывает сообщения для одного дня в файл.
            Returns: (date_str, success, error_message)
            """
            try:
                if not filtered_messages:
                    logger.debug(f"День {date_str}: сообщений не найдено, пропускаем")
                    return (date_str, True, None)

                # Сортируем по дате
                filtered_messages.sort(key=lambda x: x["msg_date"])

                # Создаем файл
                tool_output_dir = utils.OUT_DIR / "context"
                tool_output_dir.mkdir(parents=True, exist_ok=True)
                txt_path = tool_output_dir / f"context_{date_str}.txt"

                _write_context_file(txt_path, filtered_messages)

                # Если нужна сжатая версия
                if compress and txt_path.exists():
                    compressed_path = tool_output_dir / f"context_{date_str}_compressed.txt"
                    _compress_context_file(txt_path, compressed_path, min_length, max_length)

                    # Если флаг no_save_uncompressed установлен, удаляем несжатую версию
                    if no_save_uncompressed and txt_path.exists():
                        txt_path.unlink()
                        logger.debug(f"Несжатая версия удалена: {txt_path.name}")

                logger.debug(f"Завершена обработка дня: {date_str} ({len(filtered_messages)} сообщений)")
                return (date_str, True, None)
            except Exception as e:
                error_msg = f"Ошибка при обработке дня {date_str}: {e}"
                logger.error(error_msg)
                return (date_str, False, error_msg)

        # Параллельная запись файлов
        logger.info(f"Запись файлов в {actual_workers} потоках")
        completed = 0
        failed = 0
        errors = []

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Отправляем задачи для записи файлов
            future_to_date = {
                executor.submit(write_single_day, date_str, messages_by_day.get(date_str, [])): date_str
                for date_str in days_list
            }

            # Собираем результаты по мере завершения
            for future in as_completed(future_to_date):
                date_str, success, error_msg = future.result()
                if success:
                    completed += 1
                else:
                    failed += 1
                    errors.append(error_msg)

                logger.info(f"Прогресс: {completed + failed}/{total_days} дней обработано")

        # Итоговая статистика
        logger.info(f"{'=' * 60}")
        logger.info("РЕЖИМ SPLIT ЗАВЕРШЕН")
        logger.info(f"{'=' * 60}")
        logger.info(f"Всего дней: {total_days}")
        logger.info(f"Успешно обработано: {completed}")
        if failed > 0:
            logger.warning(f"Ошибок: {failed}")
            for error in errors:
                logger.warning(f"  - {error}")
        logger.info(f"Использовано потоков: {actual_workers}")
        logger.info(f"Размер батча: {batch_size} сообщений")
        logger.info(f"{'=' * 60}")
        return

    data = utils.load_json(input_path)
    msgs = data.get("messages", [])
    data.get("id", "unknown_chat_id")

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

            filtered_messages.append(
                {"from": from_name, "date_norm": date_norm, "text_plain": text_plain, "msg_date": msg_date}
            )

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
        _write_context_file(txt_path, filtered_messages)

        if txt_path.exists():
            logger.info(f"Контекстный отчет сохранен: {txt_path}")
        else:
            logger.warning("Файл не создан: за указанный период сообщений не найдено")
            return

        # Если указан флаг compress, создаем сжатую версию
        if compress and txt_path.exists():
            compressed_path = tool_output_dir / f"context_{date_str}_compressed.txt"
            logger.info("Создание сжатой версии контекста...")
            logger.info(f"Минимальная длина сообщения: {min_length} символов")
            logger.info(f"Максимальная длина сообщения: {max_length} символов")

            # Получаем статистику исходного файла
            input_size, input_chars = get_file_stats(txt_path)

            # Сжимаем файл
            _compress_context_file(txt_path, compressed_path, min_length, max_length)

            # Получаем статистику сжатого файла
            output_size, output_chars = get_file_stats(compressed_path)

            # Вычисляем процент сжатия
            compression_percent = ((input_size - output_size) / input_size) * 100 if input_size > 0 else 0
            char_compression_percent = ((input_chars - output_chars) / input_chars) * 100 if input_chars > 0 else 0

            logger.info(f"{'=' * 60}")
            logger.info("СТАТИСТИКА СЖАТИЯ:")
            logger.info(f"{'=' * 60}")
            logger.info("Исходный файл:")
            logger.info(f"  Размер: {input_size:,} байт ({input_size / 1024:.2f} КБ)")
            logger.info(f"  Символов: {input_chars:,}")
            logger.info("Сжатый файл:")
            logger.info(f"  Размер: {output_size:,} байт ({output_size / 1024:.2f} КБ)")
            logger.info(f"  Символов: {output_chars:,}")
            logger.info("Сокращение:")
            logger.info(f"  Размер: -{input_size - output_size:,} байт ({compression_percent:.2f}%)")
            logger.info(f"  Символов: -{input_chars - output_chars:,} ({char_compression_percent:.2f}%)")
            logger.info(f"{'=' * 60}")
            logger.info(f"Сжатый контекст сохранен: {compressed_path}")
            logger.info(f"{'=' * 60}")

            # Если флаг no_save_uncompressed установлен, удаляем несжатую версию
            if no_save_uncompressed and txt_path.exists():
                txt_path.unlink()
                logger.debug(f"Несжатая версия удалена: {txt_path}")

    except Exception as e:
        logger.error(f"Ошибка при создании TXT файла: {e}")
        raise
