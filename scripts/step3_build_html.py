from __future__ import annotations

import json
import logging
from pathlib import Path

from . import utils

logger = logging.getLogger(__name__)


def _load_json_if_exists(p: Path) -> dict | None:
    try:
        return utils.load_json(p)
    except Exception:
        return None


def build_html_report(
        all_agg_path: Path,
        social_graph_path: Path,
        template_name: str,
        out_html: Path
) -> None:
    logger.info("Генерация HTML-отчета")

    all_agg = _load_json_if_exists(all_agg_path)
    if not all_agg:
        logger.error("Не найден главный файл агрегатов: %s", all_agg_path)
        all_agg = {}
    else:
        logger.debug("Загружен файл агрегатов: %s", all_agg_path.name)

    social_graph = _load_json_if_exists(social_graph_path)
    if not social_graph:
        logger.warning("Файл социального графа не найден: %s", social_graph_path)
        social_graph = {}
    else:
        logger.debug("Загружен файл социального графа: %s", social_graph_path.name)

    data_blobs = {
        **all_agg,
        "social_graph": social_graph,
    }

    template_path = Path(__file__).parent.parent / "templates" / template_name

    try:
        html_template = template_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.error("Файл шаблона не найден: %s", template_path)
        html_template = f"<h1>Ошибка: шаблон не найден</h1><pre>{template_path}</pre>"
    except Exception as e:
        logger.error("Не удалось прочитать шаблон: %s", e)
        html_template = f"<h1>Ошибка чтения шаблона</h1><pre>{e}</pre>"

    html = html_template.replace("__DATA_JSON__", json.dumps(data_blobs, ensure_ascii=False))
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
