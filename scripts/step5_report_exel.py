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

import math

import orjson
import os
import pandas as pd
import logging

from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from tqdm import tqdm
from xlsxwriter.utility import xl_rowcol_to_cell
from . import utils

VALID_MESSAGE_TYPE = "message"


def safe_name(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _choose_display_name(series: pd.Series) -> str:
    """Выбираем стабильное имя: самая часто встречающаяся непустая строка, иначе первая непустая, иначе пусто."""
    s = series.fillna("").astype(str)
    s = s[s.str.len() > 0]
    if s.empty:
        return ""
    # мода (может вернуть несколько) — берём лексикографически первую для детерминизма
    modes = s.mode()
    if len(modes) > 0:
        return str(sorted(modes.astype(str).tolist())[0])
    return str(s.iloc[0])


def normalize_messages(
    raw_data: Dict[str, Any],
    logger: logging.Logger,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Преобразует НОРМАЛИЗОВАННЫЙ JSON (из step2) в DataFrame.
    Данные берутся из 'meta_norm'.
    """
    messages = []
    if isinstance(raw_data, dict) and "messages" in raw_data and isinstance(raw_data["messages"], list):
        src_iter: Iterable[Dict[str, Any]] = raw_data["messages"]
    else:
        raise ValueError("Неподдержимый формат JSON: корень не содержит массива 'messages'")

    anomalies = defaultdict(int) 

    for msg in tqdm(src_iter, desc="Чтение нормализованных сообщений (для Excel)", unit="msg"):
        if msg.get("type") != VALID_MESSAGE_TYPE:
            continue
            
        
        # Данные теперь лежат в 'meta_norm'
        meta = msg.get("meta_norm", {})

        date_norm = meta.get("date_norm")
        if not date_norm:
            anomalies["missing_date_norm"] += 1
            continue

        local_dt = datetime.fromisoformat(date_norm)
        local_date = local_dt.date()
        hour = local_dt.hour
        month = date_norm[:7] # YYYY-MM

        from_id = msg.get("from_id")
        if not from_id:
            if msg.get("type") == "service" and not msg.get("from_id"):
                pass 
            else:
                anomalies["missing_from_id"] += 1
            continue

        messages.append(
            {
                "MessageID": msg.get("id"),
                "Type": msg.get("type"),
                "LocalDT": local_dt,
                "LocalDate": local_date,
                "Hour": hour,
                "Month": month,
                "FromID": from_id,
                "Name": safe_name(msg.get("from")),
                "MediaCat": meta.get("media_cat"),
            }
        )
        

    if not messages:
        raise ValueError("После фильтрации не осталось сообщений (возможно, все были сервисными?).")

    df = pd.DataFrame(messages)
    logger.info(f"Аномалии при чтении [0].json (для Excel): {dict(anomalies)}")

    return df, dict(anomalies)

# ------------------------------ Метрики и агрегирование ------------------------------


def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Возвращает словарь со всеми производными данными (DataFrame-ы и числа), необходимые для отчёта.
    (Эта функция не требует изменений, т.к. работает с DataFrame)
    """
    # Диапазон периода
    min_dt: datetime = df["LocalDT"].min()
    max_dt: datetime = df["LocalDT"].max()
    min_date = min_dt.date()
    max_date = max_dt.date()
    total_days = (max_date - min_date).days + 1
    weeks_float = total_days / 7.0 if total_days > 0 else 0.0
    months_float = total_days / 30.4375 if total_days > 0 else 0.0

    # Активности по дням/часам
    days = (
        df.groupby("LocalDate")
        .size()
        .rename("Сообщений")
        .reset_index()
        .rename(columns={"LocalDate": "Дата"})
        .sort_values("Дата")
    )
    # часы 0..23
    hours = df.groupby("Hour").size().reindex(list(range(24)), fill_value=0).rename("Сообщений").reset_index()
    hours = hours.rename(columns={"Hour": "Час"})

    # Участники: стабильное имя
    # FromID может быть None для сервисных, добавим fillna
    df["FromID"] = df["FromID"].fillna("service_msg")
    names_by_fromid = (
        df.groupby("FromID")["Name"].apply(_choose_display_name).rename("Имя").to_dict()
    )

    df = df.copy()
    df["Имя"] = df["FromID"].map(names_by_fromid)
    by_user = df.groupby(["FromID", "Имя"]).size().rename("Сообщений").reset_index()

    total_messages = int(len(df))
    unique_users = int(df["FromID"].nunique())

    # Флаги N1/N2/M
    if weeks_float == 0:
        by_user["AvgWeek"] = 0.0
    else:
        by_user["AvgWeek"] = by_user["Сообщений"] / weeks_float
    if months_float == 0:
        by_user["AvgMonth"] = 0.0
    else:
        by_user["AvgMonth"] = by_user["Сообщений"] / months_float

    by_user["N1"] = (by_user["AvgWeek"] >= 100).astype(int)
    by_user["N2"] = (by_user["AvgMonth"] >= 1000).astype(int)
    by_user["M"] = (by_user["Сообщений"] > 10000).astype(int)
    by_user_sorted = by_user.sort_values("Сообщений", ascending=False).reset_index(drop=True)

    monthly_long = (
        df.groupby(["Month", "FromID", "Имя"])
        .size()
        .rename("Сообщений")
        .reset_index()
        .rename(columns={"Month": "Месяц"})
        .sort_values(["Месяц", "Сообщений"], ascending=[True, False])
        .reset_index(drop=True)
    )

    pivot_wide = pd.pivot_table(
        monthly_long, index=["FromID", "Имя"], columns="Месяц", values="Сообщений", aggfunc="sum", fill_value=0
    )
    pivot_wide = pivot_wide.sort_index(axis=1)
    pivot_wide = pivot_wide.reset_index()
    pivot_wide["Всего"] = pivot_wide.loc[:, pivot_wide.columns.difference(["FromID", "Имя"])].sum(axis=1)
    cols_order = ["FromID", "Имя"] + [c for c in pivot_wide.columns if c not in ("FromID", "Имя", "Всего")] + ["Всего"]
    pivot_wide = pivot_wide[cols_order]
    # Отсортируем по Всего
    pivot_wide = pivot_wide.sort_values("Всего", ascending=False).reset_index(drop=True)

    # Медиа (итоги за период)
    media_counts = Counter()
    for v in df["MediaCat"].dropna():
        media_counts[str(v)] += 1
    media_rows = []

    from .utils import MEDIA_CATEGORIES_ORDER

    for cat in MEDIA_CATEGORIES_ORDER:
        media_rows.append({"Категория": cat, "Сообщений": int(media_counts.get(cat, 0))})
    media_df = pd.DataFrame(media_rows)

    # Молчуны: < 10 сообщений
    quiet = by_user_sorted[by_user_sorted["Сообщений"] < 10].copy()
    quiet = quiet.sort_values("Сообщений", ascending=True).reset_index(drop=True)
    quiet = quiet[["FromID", "Имя", "Сообщений"]]

    metrics = {
        "df": df,
        "days": days,
        "hours": hours,
        "by_user": by_user_sorted,
        "monthly_long": monthly_long,
        "pivot_wide": pivot_wide,
        "media": media_df,
        "quiet": quiet,
        "total_messages": total_messages,
        "unique_users": unique_users,
        "min_date": min_date,
        "max_date": max_date,
        "total_days": total_days,
        "weeks_float": weeks_float,
        "months_float": months_float,
    }
    return metrics


# ------------------------------ Вывод в Excel (xlsxwriter) ------------------------------


def write_excel(
    output_path: str,
    input_path: str,
    tz_note: str, 
    hash_len: int,
    metrics: Dict[str, Any],
    anomalies: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """
    Записывает все листы и форматирование в один .xlsx
    """
    # Подготовка данных
    days: pd.DataFrame = metrics["days"]
    hours: pd.DataFrame = metrics["hours"]
    by_user: pd.DataFrame = metrics["by_user"]
    monthly_long: pd.DataFrame = metrics["monthly_long"][["Месяц", "FromID", "Имя", "Сообщений"]].copy()
    pivot_wide: pd.DataFrame = metrics["pivot_wide"].copy()
    media_df: pd.DataFrame = metrics["media"]
    quiet_df: pd.DataFrame = metrics["quiet"]

    total_messages = int(metrics["total_messages"])
    unique_users = int(metrics["unique_users"])
    min_date = metrics["min_date"]
    max_date = metrics["max_date"]
    total_days = metrics["total_days"]
    weeks_float = metrics["weeks_float"]
    months_float = metrics["months_float"]

    # Excel writer
    with pd.ExcelWriter(
        output_path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
        date_format="yyyy-mm-dd",
        datetime_format="yyyy-mm-dd hh:mm",
    ) as writer:
        workbook = writer.book

        # Форматы
        fmt_thousands = workbook.add_format({"num_format": "#,##0"})
        fmt_header = workbook.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1})
        fmt_text_wrap = workbook.add_format({"text_wrap": True})
        fmt_hint = workbook.add_format({"italic": True, "font_color": "#666666"})
        fmt_bold = workbook.add_format({"bold": True})
        fmt_date = workbook.add_format({"num_format": "yyyy-mm-dd"})
        fmt_integer = workbook.add_format({"num_format": "0"})
        fmt_center = workbook.add_format({"align": "center"})
        fmt_title = workbook.add_format({"bold": True, "font_size": 12})

        # --- Лист "Активности" ---
        ws_act = workbook.add_worksheet("Активности")
        writer.sheets["Активности"] = ws_act
        # Дни
        act_days_startrow = 0
        ws_act.write(act_days_startrow, 0, "Дни", fmt_title)
        act_days_startrow += 1
        days_cols = ["Дата", "Сообщений"]
        days.to_excel(writer, sheet_name="Активности", startrow=act_days_startrow, startcol=0, index=False)
        n_days = len(days)
        ws_act.autofilter(act_days_startrow, 0, act_days_startrow + n_days, 1)
        ws_act.set_column(0, 0, 12, fmt_date)
        ws_act.set_column(1, 1, 14, fmt_thousands)
        if n_days > 0:
            ws_act.conditional_format(
                act_days_startrow + 1, 1,
                act_days_startrow + n_days, 1,
                {"type": "data_bar"},
            )
        # Часы
        act_hours_startrow = act_days_startrow + n_days + 3
        ws_act.write(act_hours_startrow, 0, "Часы", fmt_title)
        act_hours_startrow += 1
        hours_cols = ["Час", "Сообщений"]
        hours.to_excel(writer, sheet_name="Активности", startrow=act_hours_startrow, startcol=0, index=False)
        n_hours = len(hours)
        ws_act.autofilter(act_hours_startrow, 0, act_hours_startrow + n_hours, 1)
        ws_act.set_column(0, 0, 6, fmt_integer)
        ws_act.set_column(1, 1, 14, fmt_thousands)
        if n_hours > 0:
            ws_act.conditional_format(
                act_hours_startrow + 1, 1,
                act_hours_startrow + n_hours, 1,
                {"type": "data_bar"},
            )
        ws_act.freeze_panes(act_days_startrow + 1, 0)

        # --- Лист "Топы" ---
        ws_top = workbook.add_worksheet("Топы")
        writer.sheets["Топы"] = ws_top
        cur_row = 0
        # Итог по участникам
        ws_top.write(cur_row, 0, "Итог по участникам", fmt_title)
        cur_row += 1
        cols_user = ["FromID", "Имя", "Сообщений", "AvgWeek", "AvgMonth", "N1", "N2", "M"]
        by_user_export = by_user.copy()
        by_user_export["AvgWeek"] = by_user_export["AvgWeek"].round(2)
        by_user_export["AvgMonth"] = by_user_export["AvgMonth"].round(2)
        by_user_export[cols_user].to_excel(writer, sheet_name="Топы", startrow=cur_row, startcol=0, index=False)
        n_user = len(by_user_export)
        ws_top.set_column(0, 0, 12)
        ws_top.set_column(1, 1, 24)
        ws_top.set_column(2, 2, 14, fmt_thousands)
        ws_top.set_column(3, 4, 12)
        ws_top.set_column(5, 7, 4, fmt_center)
        if n_user > 0:
            ws_top.conditional_format(cur_row + 1, 2, cur_row + n_user, 2, {"type": "data_bar"})
        cur_row = cur_row + n_user + 3
        # Помесячно (длинная)
        ws_top.write(cur_row, 0, "Помесячно (длинная)", fmt_title)
        cur_row += 1
        monthly_startrow = cur_row
        monthly_long_cols = ["Месяц", "FromID", "Имя", "Сообщений"]
        monthly_long[monthly_long_cols].to_excel(
            writer, sheet_name="Топы", startrow=monthly_startrow, startcol=0, index=False
        )
        n_monthly = len(monthly_long)
        monthly_endrow = monthly_startrow + n_monthly
        ws_top.set_column(0, 0, 9)
        ws_top.set_column(1, 1, 12)
        ws_top.set_column(2, 2, 24)
        ws_top.set_column(3, 3, 14, fmt_thousands)
        # if n_monthly >= 0:
        #     table_name = "tbl_monthly"
        #     ws_top.add_table(
        #         monthly_startrow, 0,
        #         monthly_endrow, len(monthly_long_cols) - 1,
        #         {
        #             "name": table_name,
        #             "columns": [{"header": h} for h in monthly_long_cols],
        #             "style": "Table Style Medium 2",
        #             "autofilter": False
        #         },
        #     )
        cur_row = monthly_endrow + 3
        # Широкая кросс-таблица (+ Тренд)
        ws_top.write(cur_row, 0, "Кросс-таблица (участник × месяц)", fmt_title)
        cur_row += 1
        months_cols = [c for c in pivot_wide.columns if c not in ("FromID", "Имя", "Всего")]
        wide_export = pivot_wide.copy()
        wide_export["Тренд"] = ""
        wide_startrow = cur_row
        wide_export.to_excel(writer, sheet_name="Топы", startrow=wide_startrow, startcol=0, index=False)
        n_wide = len(wide_export)
        wide_endrow = wide_startrow + n_wide
        n_wide_cols = wide_export.shape[1]
        ws_top.set_column(0, 0, 12)
        ws_top.set_column(1, 1, 24)
        col_idx = {col: i for i, col in enumerate(wide_export.columns)}
        for m in months_cols:
            ws_top.set_column(col_idx[m], col_idx[m], 10, fmt_thousands)
        ws_top.set_column(col_idx["Всего"], col_idx["Всего"], 12, fmt_thousands)
        ws_top.set_column(col_idx["Тренд"], col_idx["Тренд"], 14)
        if n_wide > 0 and len(months_cols) > 0:
            for i in range(n_wide):
                dest_cell = xl_rowcol_to_cell(wide_startrow + 1 + i, col_idx["Тренд"])
                first_val_cell = xl_rowcol_to_cell(wide_startrow + 1 + i, col_idx[months_cols[0]])
                last_val_cell = xl_rowcol_to_cell(wide_startrow + 1 + i, col_idx[months_cols[-1]])
                value_range = f"'Топы'!{first_val_cell}:{last_val_cell}"
                ws_top.add_sparkline(dest_cell, {"range": value_range})
        if n_wide > 0:
            ws_top.conditional_format(
                wide_startrow + 1, col_idx["Всего"],
                wide_endrow, col_idx["Всего"],
                {"type": "data_bar"},
            )
        ws_top.freeze_panes(1, 0)
        # Autofilter
        if 'wide_endrow' in locals() and 'monthly_endrow' in locals():
            total_rows = max(wide_endrow, monthly_endrow)
            total_cols = max(n_wide_cols, len(monthly_long_cols))
        elif 'wide_endrow' in locals():
            total_rows = wide_endrow
            total_cols = n_wide_cols
        elif 'monthly_endrow' in locals():
            total_rows = monthly_endrow
            total_cols = len(monthly_long_cols)
        else:
            total_rows = 0; total_cols = 0
        if total_rows > 0 and total_cols > 0:
            ws_top.autofilter(0, 0, total_rows, total_cols - 1)

        # --- Лист "Медиа" ---
        media_df_sorted = media_df.sort_values("Сообщений", ascending=False).reset_index(drop=True)
        media_df_sorted.to_excel(writer, sheet_name="Медиа", index=False)
        ws_media = writer.sheets["Медиа"]
        ws_media.autofilter(0, 0, len(media_df_sorted), 1)
        ws_media.set_column(0, 0, 20)
        ws_media.set_column(1, 1, 14, fmt_thousands)
        if len(media_df_sorted) > 0:
            ws_media.conditional_format(
                1, 1,
                len(media_df_sorted), 1,
                {"type": "data_bar"},
            )

        # --- Лист "Молчуны" ---
        quiet_df.to_excel(writer, sheet_name="Молчуны", index=False)
        ws_quiet = writer.sheets["Молчуны"]
        ws_quiet.autofilter(0, 0, len(quiet_df), 2)
        ws_quiet.set_column(0, 0, 12)
        ws_quiet.set_column(1, 1, 24)
        ws_quiet.set_column(2, 2, 14, fmt_thousands)

        # ---------------- Общее ----------------
        ws_sum = workbook.add_worksheet("Общее")
        writer.sheets["Общее"] = ws_sum
        row = 0

        ws_sum.write(row, 0, "Параметры", fmt_title)
        row += 1
        gen_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        
        # Обновляем часовой пояс и период
        params = [
            ("Имя входного файла", os.path.basename(input_path)),
            ("Дата/время генерации", gen_time),
            ("Часовой пояс", tz_note),
            (f"Период ({tz_note.split(' ')[0]})", f"{min_date} — {max_date}"),
        ]
        
        
        for k, v in params:
            ws_sum.write(row, 0, k)
            ws_sum.write(row, 1, v)
            row += 1

        row += 1
        ws_sum.write(row, 0, "Итоги по чату", fmt_title)
        row += 1
        totals = [
            ("Всего сообщений", total_messages, fmt_thousands),
            ("Уникальных участников", unique_users, fmt_thousands),
            ("Дней в периоде (TotalDays)", total_days, fmt_integer),
            ("Среднее сообщений в день", (total_messages / total_days) if total_days > 0 else 0.0, None),
            ("Недели (WeeksFloat)", weeks_float, None),
            ("Месяцы (MonthsFloat)", months_float, None),
        ]
        for k, v, fm in totals:
            ws_sum.write(row, 0, k)
            if fm is None:
                ws_sum.write_number(row, 1, float(v))
            else:
                ws_sum.write_number(row, 1, float(v) if isinstance(v, float) else int(v), fm)
            row += 1

        row += 1
        ws_sum.write(row, 0, "Динамика по дням (sparkline)", fmt_title)
        # Sparkline
        if len(days) > 0:
            spark_cell = xl_rowcol_to_cell(row, 1)
            days_values_first = xl_rowcol_to_cell(act_days_startrow + 1, 1)
            days_values_last = xl_rowcol_to_cell(act_days_startrow + n_days, 1)
            value_range = f"'Активности'!{days_values_first}:{days_values_last}"
            ws_sum.add_sparkline(spark_cell, {"range": value_range})
        row += 2

        # Валидации/аномалии
        ws_sum.write(row, 0, "Валидации / аномалии (при чтении [0].json)", fmt_title)
        row += 1
        
        anomaly_counts = [
            ("Отсутствующая/невалидная дата", anomalies.get("missing_date_norm", 0)),
            ("Пустой/некорректный from_id", anomalies.get("missing_from_id", 0)),
        ]
        
        ws_sum.write(row, 0, "Тип")
        ws_sum.write(row, 1, "Счётчик")
        ws_sum.set_row(row, None, fmt_header)
        row += 1
        for k, v in anomaly_counts:
            ws_sum.write(row, 0, k)
            ws_sum.write_number(row, 1, int(v), fmt_thousands)
            row += 1
        row += 1
        
        ws_sum.set_column(0, 0, 40)
        ws_sum.set_column(1, 1, 40)

        # ---------------- Сводная (PivotTable) ----------------
        ws_pivot = workbook.add_worksheet("Сводная")
        writer.sheets["Сводная"] = ws_pivot
        
        ws_pivot.write(0, 0, "Данные для создания сводной таблицы", fmt_title)
        ws_pivot.write(1, 0, "Экспортируем данные в формате, удобном для создания PivotTable в Excel", fmt_hint)
        
        pivot_data_startrow = 3
        pivot_cols = ["Месяц", "FromID", "Имя", "Сообщений"]
        
        for i, col in enumerate(pivot_cols):
            ws_pivot.write(pivot_data_startrow, i, col, fmt_header)
        
        monthly_long[pivot_cols].to_excel(
            writer, 
            sheet_name="Сводная", 
            startrow=pivot_data_startrow + 1, 
            startcol=0, 
            index=False
        )
        
        n_pivot_rows = len(monthly_long)
        ws_pivot.set_column(0, 0, 12, fmt_date)
        ws_pivot.set_column(1, 1, 12)
        ws_pivot.set_column(2, 2, 24)
        ws_pivot.set_column(3, 3, 14, fmt_thousands)
        
        if n_pivot_rows > 0:
            ws_pivot.autofilter(pivot_data_startrow, 0, pivot_data_startrow + n_pivot_rows, len(pivot_cols) - 1)
        
        instruction_startrow = pivot_data_startrow + n_pivot_rows + 3
        ws_pivot.write(instruction_startrow, 0, "Инструкция по созданию PivotTable:", fmt_bold)
        instruction_startrow += 1
        
        instructions = [
            "1. Выделите все данные (включая заголовки)",
            "2. Вставка → Сводная таблица",
            "3. Настройте поля:",
            "   - Строки: FromID, Имя",
            "   - Столбцы: Месяц", 
            "   - Значения: Сообщений (сумма)",
            "4. При необходимости настройте сортировку и фильтры"
        ]
        
        for i, instruction in enumerate(instructions):
            ws_pivot.write(instruction_startrow + i, 0, instruction)
        
        ws_pivot.set_column(0, 0, 50)

        logger.info("Excel сохранён: %s", output_path)


def generate_excel_report(
    normalized_json_path: Path, 
    output_excel_path: Path,
    hash_len: int, 
    logger: logging.Logger
) -> None:
    """
    Основная функция-обертка для запуска генерации Excel
    из нормализованного JSON-файла.
    """

    logger.info("Загрузка нормализованного JSON: %s", normalized_json_path)
    if not normalized_json_path.exists():
        logger.error("Файл не найден: %s", normalized_json_path)
        raise FileNotFoundError(normalized_json_path)

    with normalized_json_path.open("rb") as f:
         raw = orjson.loads(f.read())

    
    # Читаем 'shift' и 'note' из meta-блока, который создал step2
    meta_info_list = raw.get("meta", [])
    meta_info = {}
    if meta_info_list and isinstance(meta_info_list, list):
        # Ищем наш блок 'by_normalize'
        for item in meta_info_list:
            if "by_normalize" in item:
                meta_info = item.get("by_normalize", {})
                break

    shift = meta_info.get("applied_shift_hours", 0)
    tz_note = meta_info.get("note", f"UTC{shift:+d}")
    

    df, anomalies = normalize_messages(raw, logger)

    logger.info("Расчёт метрик для Excel...")
    metrics = compute_metrics(df)
    logger.info(
        "Обработано сообщений: %d; период: %s — %s; участников: %d",
        metrics["total_messages"],
        metrics["min_date"],
        metrics["max_date"],
        metrics["unique_users"],
    )

    # Передаем 'tz_note' вместо 'tz_shift_hours'
    write_excel(
        output_path=str(output_excel_path),
        input_path=str(normalized_json_path), 
        tz_note=tz_note, 
        hash_len=hash_len,
        metrics=metrics,
        anomalies=anomalies,
        logger=logger,
    )
    logger.info("Отчёт Excel сохранён: %s", output_excel_path)