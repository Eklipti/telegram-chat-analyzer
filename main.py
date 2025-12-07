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

    p3 = sub.add_parser("agg", help="Агрегаты (JSON) -> /output/agg")
    p3.add_argument("--input", type=Path)
    p3.add_argument("--out-dir", type=Path)

    p4 = sub.add_parser("html", help="Единый HTML-отчёт -> /output/report.html")
    p4.add_argument("--agg-dir", type=Path, help="Каталог агрегатов (по умолч. /output/agg)")
    p4.add_argument("--out",     type=Path, help="Путь к report.html (по умолч. /output/report.html)")

    p5 = sub.add_parser("mobile", help="Мобильный HTML-отчёт -> /output/report.mobile.html")
    p5.add_argument("--agg-dir", type=Path, help="Каталог агрегатов (по умолч. /output/agg)")
    p5.add_argument("--out",     type=Path, help="Путь к report.mobile.html (по умолч. /output/report.mobile.html)")

    p6 = sub.add_parser("all", help="Полный конвейер: normalize -> agg -> social -> html -> mobile -> excel")
    p6.add_argument("--input", type=Path)

    p7 = sub.add_parser("social", help="Социальный граф и взаимодействия -> /output/agg/social_graph.json")
    p7.add_argument("--input", type=Path, help="Входной JSON (нормализованный)")
    p7.add_argument("--out-dir", type=Path, help="Каталог для сохранения (по умолч. /output/agg)")

    p8 = sub.add_parser("author_and_text", help="Спец. отчет: Авторы и тексты -> /output/author_text.json")
    p8.add_argument("--input", type=Path, help="Входной JSON (нормализованный или сырой)")
    p8.add_argument("--out", type=Path, help="Путь для сохранения результата")

    args = ap.parse_args()

    if args.cmd == "params":
        from scripts.tool_params import generate_params_md
        src = utils.find_input_json(args.input)
        out = args.output or (utils.OUT_DIR / "md" / "json_params.md")
        generate_params_md(src, out)

    elif args.cmd == "author_and_text":
        try:
            src = utils.find_normalized_json(args.input)
            logger.info(f"Используем нормализованный файл: {src}")
        except FileNotFoundError:
            logger.warning("Нормализованный файл не найден. Ищем сырой файл для обработки...")
            raw = utils.find_input_json(args.input)
            src = normalize_json(raw, None) # normalize вернет путь к созданному файлу
            logger.info(f"Файл нормализован: {src}")

        out_file = args.out or (utils.OUT_DIR / "author_text" / "author_text_report.json")

        generate_author_text_report(src, out_file)

    elif args.cmd == "normalize":
        src = utils.find_input_json(args.input)
        normalize_json(src, None)

    elif args.cmd == "agg":
        out_dir = args.out_dir or utils.AGG_DIR
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            raw = utils.find_input_json(args.input)
            src0 = normalize_json(raw, None)
        build_aggregates_json(src0, out_dir)

    elif args.cmd == "social":
        out_dir = args.out_dir or utils.AGG_DIR
        try:
            src0 = utils.find_normalized_json(args.input)
        except FileNotFoundError:
            logger.error("Нормализованный файл не найден. Запустите 'normalize' или 'all' сначала.")
            return
        build_social_graph(src0, out_dir)

    elif args.cmd == "html":
        agg_dir = args.agg_dir or utils.AGG_DIR
        out     = args.out     or (utils.OUT_DIR / "report.html")
        build_html_report(
            agg_dir=agg_dir, 
            template_name="desktop.html", 
            out_html=out
        )

    elif args.cmd == "mobile":
        agg_dir = args.agg_dir or utils.AGG_DIR
        out     = args.out     or (utils.OUT_DIR / "report.mobile.html")
        build_html_report(
            agg_dir=agg_dir, 
            template_name="mobile.html", 
            out_html=out
        )

    elif args.cmd == "all":
        utils.AGG_DIR.mkdir(parents=True, exist_ok=True)
        utils.OUT_DIR.mkdir(parents=True, exist_ok=True)

        src_raw = utils.find_input_json(args.input)

        logger.info("--- ШАГ 1: Нормализация данных ---")
        try:
            norm_path = normalize_json(src_raw, None)
        except Exception as e:
            logger.error(f"--- ОШИБКА: Шаг 1 не удался ---")
            logger.error(e, exc_info=True)
            return 

        logger.info("--- ШАГ 2: Агрегация ---")
        build_aggregates_json(norm_path, utils.AGG_DIR)

        logger.info("--- ШАГ 3.5: Социальный граф ---")
        build_social_graph(norm_path, utils.AGG_DIR)

        logger.info("--- ШАГ 3: Генерация HTML-отчётов ---")
        build_html_report(
            agg_dir=utils.AGG_DIR, 
            template_name="desktop.html", 
            out_html=(utils.OUT_DIR / "report.html")
        )
        build_html_report(
            agg_dir=utils.AGG_DIR, 
            template_name="mobile.html", 
            out_html=(utils.OUT_DIR / "report.mobile.html")
        )

        logger.info("--- ШАГ 4: Генерация Excel-отчета ---")
        generate_excel_report(
            normalized_json_path=norm_path,
            output_excel_path=(utils.OUT_DIR / "report.xlsx"),
            hash_len=10,
            logger=logger
        )

        logger.info("--- ПРОЦЕСС ЗАВЕРШЁН ---")

if __name__ == "__main__":
    main()