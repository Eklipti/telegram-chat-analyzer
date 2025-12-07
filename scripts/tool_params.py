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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple
from . import utils

def _value_type_name(v: Any) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "bool"
    if isinstance(v, int): return "int"
    if isinstance(v, float): return "float"
    if isinstance(v, str): return "str"
    if isinstance(v, list): return "list"
    if isinstance(v, dict): return "dict"
    return type(v).__name__

def _walk(obj: Any, key_counter, path_counter, type_counter, array_items_total, array_containers_count, path: str = "$") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key_counter[k] += 1
            p = f"{path}.{k}"
            path_counter[p] += 1
            type_counter[p][_value_type_name(v)] += 1
            _walk(v, key_counter, path_counter, type_counter, array_items_total, array_containers_count, p)
    elif isinstance(obj, list):
        list_item_path = f"{path}[]"
        array_containers_count[list_item_path] += 1
        array_items_total[list_item_path] += len(obj)
        for item in obj:
            path_counter[list_item_path] += 1
            type_counter[list_item_path][_value_type_name(item)] += 1
            _walk(item, key_counter, path_counter, type_counter, array_items_total, array_containers_count, list_item_path)

def _md_header(t: str, lvl: int = 1) -> str:
    return f"{'#'*lvl} {t}\n\n"

def _table(rows: Iterable[Tuple[str, ...]], headers: Tuple[str, ...]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "| " + " | ".join("---" for _ in headers) + " |"]
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out) + "\n\n"

def _pct(part: int, total: int) -> str:
    return "0.00%" if not total else f"{(part/total)*100:.2f}%"

def generate_params_md(input_path: Path, output_md: Path) -> None:
    meta_shift = utils.parse_filename_shift(input_path)
    data = utils.load_json(input_path)

    key_counter, path_counter = Counter(), Counter()
    type_counter: Dict[str, Counter] = defaultdict(Counter)
    array_items_total, array_containers_count = Counter(), Counter()
    root_snapshot = {}

    if isinstance(data, dict):
        for k, v in data.items():
            root_snapshot[k] = _value_type_name(v)
    _walk(data, key_counter, path_counter, type_counter, array_items_total, array_containers_count, "$")

    # сводка
    total_msgs = int(path_counter.get("$.messages[]", 0))  # Количество элементов массива messages
    replies = int(path_counter.get("$.messages[].reply_to_message_id", 0))
    edited = int(path_counter.get("$.messages[].edited", 0))
    reactions_msgs = int(path_counter.get("$.messages[].reactions", 0))
    with_photo = int(path_counter.get("$.messages[].photo", 0))
    with_file = int(path_counter.get("$.messages[].file", 0))

    lines = []
    lines.append(_md_header("Параметры JSON экспорта Telegram"))
    lines.append(_md_header("Метаданные файла", 2))
    meta_rows = [
        ("Имя файла", input_path.name),
        ("Размер, МБ", f"{input_path.stat().st_size / (1024*1024):.2f}"),
        ("Сгенерировано (UTC)", datetime.utcnow().isoformat(timespec="seconds")+"Z"),
        ("Shift к МСК (из имени)", meta_shift if meta_shift is not None else "—"),
    ]
    lines.append(_table(meta_rows, ("Поле","Значение")))

    lines.append(_md_header("Сводка по сообщениям", 2))
    lines.append(_table([
        ("Всего сообщений", total_msgs),
        ("Ответы", f"{replies} ({_pct(replies,total_msgs)})"),
        ("Отредактированы", f"{edited} ({_pct(edited,total_msgs)})"),
        ("С реакциями", f"{reactions_msgs} ({_pct(reactions_msgs,total_msgs)})"),
        ("С фото", with_photo),
        ("С файлами", with_file),
    ], ("Метрика","Значение")))

    lines.append(_md_header("Ключи верхнего уровня (типы)", 2))
    lines.append(_table(sorted(root_snapshot.items()), ("Ключ","Тип")))

    lines.append(_md_header("Частота имён ключей", 2))
    lines.append(_table([(k,c) for k,c in key_counter.most_common()], ("Ключ","Количество")))

    lines.append(_md_header("Частота полных путей", 2))
    path_rows=[]
    for p,c in path_counter.most_common():
        types_str = ", ".join(f"{t}:{n}" for t,n in type_counter.get(p, Counter()).most_common())
        path_rows.append((p,c,types_str))
    lines.append(_table(path_rows, ("Путь","Количество","Типы (count)")))

    lines.append(_md_header("Пути массивов (occurrences / total items / avg)", 2))
    arr=[]
    for p in sorted([k for k in path_counter if k.endswith("[]")]):
        # Количество элементов массива (сколько раз встретился путь path[])
        occ = int(path_counter.get(p, 0))
        # Количество контейнеров-массивов (сколько было списков по этому пути)
        num_arrays = int(array_containers_count.get(p, 0))
        # Сумма длин всех массивов по этому пути
        total = int(array_items_total.get(p, 0))
        # Среднее количество элементов в массиве
        avg = f"{(total/num_arrays):.2f}" if num_arrays > 0 else "—"
        types_str = ", ".join(f"{t}:{n}" for t,n in type_counter.get(p, Counter()).most_common())
        arr.append((p, num_arrays, occ, total, avg, types_str))
    lines.append(_table(arr, ("Путь","Списков","Элементов","Сумма длин","Среднее","Типы/счётчики")))

    lines.append(_md_header("Примечания", 2))
    du = ", ".join(f"{t}:{n}" for t,n in type_counter.get("$.messages[].date_unixtime", Counter()).most_common())
    eu = ", ".join(f"{t}:{n}" for t,n in type_counter.get("$.messages[].edited_unixtime", Counter()).most_common())
    lines.append(
        "- Количество = число вхождений поля.\n"
        "- Пути `[]` показывают списки: сколько встретилось контейнеров и суммарно элементов.\n"
        f"- Типы `date_unixtime`: {du or '—'}; `edited_unixtime`: {eu or '—'}.\n"
    )

    tool_output_dir = utils.OUT_DIR / "params"
    tool_output_dir.mkdir(parents=True, exist_ok=True)
    output_md = tool_output_dir / output_md.name
    output_md.write_text("".join(lines), encoding="utf-8")
