import argparse
import logging
from pathlib import Path

from scripts import utils
from scripts.step1_normalize import normalize_json
from scripts.step2_aggregates import build_aggregates_json
from scripts.step3_5_social_graph import build_social_graph
from scripts.step3_build_html import build_html_report
from scripts.step4_report_exel import generate_excel_report
from scripts.tool_author_text import generate_author_text_report
from scripts.tool_context import generate_context_report


def main():

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("main")

    ap = argparse.ArgumentParser(description="Telegram export (JSON-only)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("params", help="Скан и генерация json_params.md (Опционально)")
    p1.add_argument("--input", type=Path)
    p1.add_argument("--output", type=Path)

    p2 = sub.add_parser("normalize", help="Нормализация [raw] -> [processed]")
    p2.add_argument("--input", type=Path)
    p2.add_argument("--force", action="store_true", help="Перезаписать существующий нормализованный файл")

    p3 = sub.add_parser("agg", help="Агрегаты (JSON) -> /output/agg")
    p3.add_argument("--input", type=Path)
    p3.add_argument("--out-dir", type=Path)

    p4 = sub.add_parser("html", help="Единый HTML-отчёт -> /output/report.html")
    p4.add_argument("--input", type=Path, help="Входной JSON для определения хэш-папки")
    p4.add_argument("--agg-dir", type=Path, help="Каталог агрегатов (по умолч. /output/agg)")
    p4.add_argument("--out", type=Path, help="Путь к report.html (по умолч. /output/report.html)")

    p5 = sub.add_parser("mobile", help="Мобильный HTML-отчёт -> /output/report.mobile.html")
    p5.add_argument("--input", type=Path, help="Входной JSON для определения хэш-папки")
    p5.add_argument("--agg-dir", type=Path, help="Каталог агрегатов (по умолч. /output/agg)")
    p5.add_argument("--out", type=Path, help="Путь к report.mobile.html (по умолч. /output/report.mobile.html)")

    p6 = sub.add_parser("all", help="Полный конвейер: normalize -> agg -> social -> html -> mobile -> excel")
    p6.add_argument("--input", type=Path)
    p6.add_argument(
        "--force", action="store_true", help="Перезаписать существующий нормализованный файл на шаге normalize"
    )

    p7 = sub.add_parser("social", help="Социальный граф и взаимодействия -> /output/agg/social_graph.json")
    p7.add_argument("--input", type=Path, help="Входной JSON (нормализованный)")
    p7.add_argument("--out-dir", type=Path, help="Каталог для сохранения (по умолч. /output/agg)")

    p8 = sub.add_parser("author_and_text", help="Спец. отчет: Авторы и тексты -> /output/author_text.json")
    p8.add_argument("--input", type=Path, help="Входной JSON (нормализованный или сырой)")
    p8.add_argument("--out", type=Path, help="Путь для сохранения результата")

    p9 = sub.add_parser("context", help="Текстовая история сообщений за период -> /output/context/context_*.txt")
    p9.add_argument(
        "--date",
        type=str,
        required=True,
        help="Дата или период: -1 (вчера), YYYY-MM-DD (день), YYYY-MM-DD_YYYY-MM-DD (период)",
    )
    p9.add_argument("-i", "--input", type=Path, help="Путь к нормализованному JSON (опционально)")
    p9.add_argument("--compress", action="store_true", help="Создать сжатую версию контекста")
    p9.add_argument("--no-save", action="store_true",
                    help="Не сохранять несжатую версию (работает только с --compress)")
    p9.add_argument(
        "-s",
        "--split",
        action="store_true",
        help="Разбить период на отдельные файлы для каждого дня (работает только с периодом)",
    )
    p9.add_argument(
        "-t",
        "--threads",
        type=int,
        default=2,
        help="Количество потоков для режима --split (по умолчанию 2, максимум 100)",
    )
    p9.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Размер батча сообщений для обработки в режиме --split (по умолчанию 10000 строк)",
    )
    p9.add_argument("--min", type=int, default=5, help="Минимальная длина сообщения для сжатой версии (по умолчанию 5)")
    p9.add_argument(
        "--max", type=int, default=250, help="Максимальная длина сообщения для сжатой версии (по умолчанию 250)"
    )

    p10 = sub.add_parser("excel", help="Excel-отчёт -> /output/report.xlsx")
    p10.add_argument("--input", type=Path, help="Входной JSON (нормализованный или сырой)")
    p10.add_argument("--out", type=Path, help="Путь к report.xlsx (по умолч. /output/report.xlsx)")

    args = ap.parse_args()

    if args.cmd == "params":
        from scripts.tool_params import generate_params_md

        src = utils.find_input_json(args.input)
        utils.init_hashed_output_dir(src)
        out = args.output or (utils.OUT_DIR / "md" / "json_params.md")
        generate_params_md(src, out)

    elif args.cmd == "author_and_text":
        try:
            src = utils.find_normalized_json(args.input)
            logger.info(f"Используем нормализованный файл: {src}")
        except FileNotFoundError:
            logger.warning("Нормализованный файл не найден. Ищем сырой файл для обработки...")
            raw = utils.find_input_json(args.input)
            src = normalize_json(raw, None, "user" if args.input else "auto")
            logger.info(f"Файл нормализован: {src}")

        utils.init_hashed_output_dir(src)
        out_file = args.out or (utils.OUT_DIR / "author_text" / "author_text_report.json")

        generate_author_text_report(src, out_file)

    elif args.cmd == "context":
        try:
            src = utils.find_normalized_json(args.input)
            logger.info(f"Используем нормализованный файл: {src}")
        except FileNotFoundError:
            logger.warning("Нормализованный файл не найден. Ищем сырой файл для обработки...")
            raw = utils.find_input_json(args.input)
            src = normalize_json(raw, None, "user" if args.input else "auto")
            logger.info(f"Файл нормализован: {src}")

        utils.init_hashed_output_dir(src)
        # output_path используется только для совместимости с сигнатурой функции,
        # но фактический путь формируется внутри функции на основе даты
        out_file = None

        generate_context_report(
            src,
            out_file,
            args.date,
            compress=args.compress,
            no_save_uncompressed=args.no_save,
            split_by_days=args.split,
            max_workers=args.threads,
            batch_size=args.batch_size,
            min_length=args.min,
            max_length=args.max,
        )

    elif args.cmd == "excel":
        try:
            src = utils.find_normalized_json(args.input)
            logger.info(f"Используем нормализованный файл: {src}")
        except FileNotFoundError:
            logger.warning("Нормализованный файл не найден. Ищем сырой файл для обработки...")
            raw = utils.find_input_json(args.input)
            src = normalize_json(raw, None, "user" if args.input else "auto")
            logger.info(f"Файл нормализован: {src}")

        utils.init_hashed_output_dir(src)
        out_file = args.out or (utils.OUT_DIR / "report.xlsx")

        generate_excel_report(normalized_json_path=src, output_excel_path=out_file, hash_len=10, logger=logger)

    elif args.cmd == "normalize":
        src = utils.find_input_json(args.input)
        normalize_json(src, None, "user" if args.input else "auto", force=getattr(args, "force", False))

    elif args.cmd == "agg":
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            raw = utils.find_input_json(args.input)
            src0 = normalize_json(raw, None, "user" if args.input else "auto")
        utils.init_hashed_output_dir(src0)
        out_dir = args.out_dir or utils.AGG_DIR
        build_aggregates_json(src0, out_dir)

    elif args.cmd == "social":
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            logger.error("Нормализованный файл не найден. Запустите 'normalize' или 'all' сначала.")
            return
        utils.init_hashed_output_dir(src0)
        out_dir = args.out_dir or utils.AGG_DIR
        build_social_graph(src0, out_dir)

    elif args.cmd == "html":
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            src0 = utils.find_input_json(args.input)
        utils.init_hashed_output_dir(src0)

        agg_dir = args.agg_dir or utils.AGG_DIR
        out = args.out or (utils.OUT_DIR / "report.html")
        build_html_report(
            all_agg_path=agg_dir / "all_aggregates.json",
            social_graph_path=agg_dir / "social_graph.json",
            template_name="desktop.html",
            out_html=out,
        )

    elif args.cmd == "mobile":
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            src0 = utils.find_input_json(args.input)
        utils.init_hashed_output_dir(src0)

        agg_dir = args.agg_dir or utils.AGG_DIR
        out = args.out or (utils.OUT_DIR / "report.mobile.html")
        build_html_report(
            all_agg_path=agg_dir / "all_aggregates.json",
            social_graph_path=agg_dir / "social_graph.json",
            template_name="mobile.html",
            out_html=out,
        )

    elif args.cmd == "all":
        src_raw = utils.find_input_json(args.input)
        utils.init_hashed_output_dir(src_raw)

        logger.info("--- ШАГ 1: Нормализация данных ---")
        try:
            norm_path = normalize_json(
                src_raw, None, "user" if args.input else "auto", force=getattr(args, "force", False)
            )
        except Exception as e:
            logger.error("--- ОШИБКА: Шаг 1 не удался ---")
            logger.error(e, exc_info=True)
            return

        logger.info("--- ШАГ 2: Агрегация ---")
        build_aggregates_json(norm_path, utils.AGG_DIR)

        logger.info("--- ШАГ 3.5: Социальный граф ---")
        build_social_graph(norm_path, utils.AGG_DIR)

        logger.info("--- ШАГ 3: Генерация HTML-отчётов ---")
        build_html_report(
            all_agg_path=utils.AGG_DIR / "all_aggregates.json",
            social_graph_path=utils.AGG_DIR / "social_graph.json",
            template_name="desktop.html",
            out_html=(utils.OUT_DIR / "report.html"),
        )
        build_html_report(
            all_agg_path=utils.AGG_DIR / "all_aggregates.json",
            social_graph_path=utils.AGG_DIR / "social_graph.json",
            template_name="mobile.html",
            out_html=(utils.OUT_DIR / "report.mobile.html"),
        )

        logger.info("--- ПРОЦЕСС ЗАВЕРШЁН ---")


if __name__ == "__main__":
    main()
