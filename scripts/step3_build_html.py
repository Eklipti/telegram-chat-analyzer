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
from pathlib import Path
from typing import Optional
import json
from . import utils

def _load_json_if_exists(p: Path) -> Optional[dict]:
    try:
        return utils.load_json(p)
    except Exception:
        return None

def build_html_report(
    agg_dir: Path, 
    template_name: str, # "desktop.html" или "mobile.html"
    out_html: Path
) -> None:
    
    all_agg_path = agg_dir / "all_aggregates.json"
    all_agg = _load_json_if_exists(all_agg_path)
    if not all_agg:
        print(f"ОШИБКА: Не найден главный файл агрегатов: {all_agg_path}")
        all_agg = {}

    # Загрузку .md файлов убрал
    # data_blobs - это просто копия all_agg
    data_blobs = {
        **all_agg,
    }

    try:
        template_path = Path(__file__).parent.parent / "templates" / template_name
        html_template = template_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        print(f"ОШИБКА: Файл шаблона не найден: {template_path}")
        html_template = f"<h1>Ошибка: шаблон не найден</h1><pre>{template_path}</pre>"
    except Exception as e:
        print(f"ОШИБКА: Не удалось прочитать шаблон: {e}")
        html_template = f"<h1>Ошибка чтения шаблона</h1><pre>{e}</pre>"

    html = html_template.replace("__DATA_JSON__", json.dumps(data_blobs, ensure_ascii=False))
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")