# Telegram Chat Analyzer

Инструмент для анализа чатов Telegram на основе JSON-экспорта из Desktop приложения.

- [Возможности и метрики](docs/features.md) — подробное описание того, что умеет анализатор.
- [Команды и утилиты](docs/tutorials.md) — как запускать основной конвейер и вспомогательные инструменты.
- [Примеры результатов](examples) — как выглядят артефакты на выходе.

---

## Установка

1. Установите [Python](https://www.python.org/downloads) 3.8 или выше. 
2. Клонируйте репозиторий:
    ```powershell
    git clone https://github.com/Eklipti/telegram-chat-analyzer
    cd telegram-chat-analyzer
    ```
3. Создайте и активируйте виртуальное окружение:
    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

4. Установите зависимости:
    ```powershell
    pip install -r requirements.txt
    ```

> **Примечание:** Для анализа словарного запаса используется `pymorphy3`. Если библиотека недоступна, анализ будет выполнен без лемматизации.

---

## Быстрый старт

1. Получите экспорт истории чата в формате **JSON** через Telegram Desktop.
2. Поместите файл (например, `result.json`) в папку `telegram/exports/raw_json/`.
3. Запустите полный конвейер. Скрипт автоматически найдет самый свежий файл в папке `raw_json/`. Или укажите нужный через `--input`.

```powershell
python main.py all
```

Подробное описание всех команд и их параметров — в [tutorials.md](docs/tutorials.md).

### Результаты
После завершения в папке `output/` появятся отчеты.

**Примеры файлов:**
- [Сырой JSON](examples/raw[+5].json)
- [Обогащённый JSON](examples/raw[0].json)
- [Агрегированные данные](examples/all_aggregates.json)
- [Социальный граф](examples/social_graph.json)
- [HTML отчёт (desktop)](examples/report.html) | [HTML отчёт (mobile)](examples/report.mobile.html)
- [Excel отчёт (.xlsx)](examples/report.xlsx)

---

## Лицензия

Проект распространяется по лицензии «Take it, lol», то есть [MIT](LICENSE).
Copyright (C) 2025-2026 Eklipti.
