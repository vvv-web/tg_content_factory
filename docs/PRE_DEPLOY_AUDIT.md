# Pre-Deploy Audit — TG Agent (tg-user-search)

**Дата:** 2025-03-09  
**Версия:** 0.1.6  
**Роль:** DevOps/QA инженер

---

## 1. Краткая сводка проекта

**TG Agent** — персональный тулкит мониторинга Telegram: сбор сообщений из каналов, поиск (локальный FTS5, Telegram API, AI/LLM), уведомления по ключевым словам. Стек: Python 3.11+, FastAPI, SQLite, Telethon, APScheduler.

**Ключевые возможности:**
- Мультиаккаунт с ротацией при flood-wait
- Очередь сбора (CollectionQueue) + планировщик (APScheduler)
- Веб-панель (Basic Auth + cookie-сессии, CSRF)
- Шифрование Telegram session strings (Fernet + PBKDF2)
- Docker-ready (Dockerfile + docker-compose)

**Архитектура:** CLI/Web → Telegram + Search + Scheduler → SQLite (одна БД в `data/tg_search.db`).

---

## 2. Секреты и окружение

### Переменные окружения (.env)

| Переменная | Обязательна | Описание |
|------------|-------------|----------|
| `TG_API_ID` | Да | Telegram API ID (my.telegram.org/apps) |
| `TG_API_HASH` | Да | Telegram API Hash |
| `WEB_PASS` | Да | Пароль веб-панели (без него serve не стартует) |
| `SESSION_ENCRYPTION_KEY` | Нет* | Ключ шифрования session strings в БД |
| `LLM_API_KEY` | Нет | API-ключ для AI-поиска |

\* Если в БД есть зашифрованные сессии (`enc:v1:*` или `enc:v2:*`) и ключ не задан — **старт падает** (`RuntimeError` в `Database.initialize()`).

### Конфигурация (config.yaml)

- Подстановка `${ENV_VAR}`; пустые env-переменные **удаляют ключ** (`_walk_and_substitute`).
- Fallback: если `config.yaml` отсутствует — создаётся `AppConfig()` с дефолтами.
- Telegram-учётные данные можно задать через env (`TG_API_ID`, `TG_API_HASH`) — `config.py` читает их при пустом конфиге.

### Секреты, хранимые в БД (settings)

- `session_secret_key` — HMAC для cookie; генерируется при первом запуске (`secrets.token_hex(32)`).
- `tg_api_id`, `tg_api_hash` — при сохранении через UI.
- `notification_account_phone` — выбранный аккаунт для уведомлений.

### Рекомендации по секретам

- `SESSION_ENCRYPTION_KEY` **рекомендуется** задавать до добавления первого аккаунта.
- `.env` и `config.yaml` в `.gitignore` не попадают; `.env` исключён.
- `config.yaml` содержит плейсхолдеры `${...}` — значения берутся из env при загрузке.

---

## 3. Тесты

### Общие сведения

- **524 теста** — все проходят (pytest, pytest-asyncio, timeout 30s).
- `testpaths = ["tests"]`, `asyncio_mode = "auto"`.

### Что покрыто

| Область | Файл(ы) | Охват |
|---------|---------|-------|
| Веб: auth, CSRF, redirects | test_web.py | Basic/cookie, open redirect, CSRF (Origin), logout |
| Веб: страницы, формы | test_web.py | Dashboard, settings, channels, search, scheduler, filter |
| Collection queue | test_web.py, test_integration | Enqueue, cancel, filtered/inactive skip, requeue |
| Scheduler | test_scheduler.py | Interval, trigger, jobs |
| Database, migrations | test_database.py | Init, encrypted sessions fail without key, migrations |
| Session cipher | test_session_cipher.py | encrypt/decrypt v1/v2 |
| Config | test_config.py | Env substitution, empty var drop |
| Парсеры, импорт | test_parsers.py, test_import_web | Идентификаторы, txt/csv/xlsx |
| Collector, client pool | test_collector.py, test_client_pool | Flood wait, rotation |
| Фильтры | test_filters.py | Uniqueness, subscriber ratio |
| Поиск | test_search.py, test_search_queries | Локальный, AI |
| Notification | test_notification.py | Setup, delete, status |
| Интеграция | test_integration.py | Полные сценарии через web layer |

### Чего не хватает перед развёртыванием

- **Docker**: нет тестов запуска в контейнере, healthcheck через Docker.
- **Persist/restart**: нет тестов на `requeue_startup_tasks` после «краша» worker’а.
- **SQLite WAL**: нет тестов на конкурентный доступ под нагрузкой.
- **Backup/restore**: логика бэкапов не тестируется.
- **Rate limits / brute-force**: нет тестов на защиту логина (только compare_digest).
- **Security**: нет тестов на injection (SQL — везде параметризовано, но явных тестов нет).

---

## 4. Deployment Risks (по приоритету)

### Критичные (P1)

1. **Telegram session encryption**
   - **Риск:** Миграция с plaintext → encrypted: при смене `SESSION_ENCRYPTION_KEY` старые сессии не расшифруются.
   - **Смягчение:** Ключ задавать до добавления аккаунтов; не менять после появления encrypted sessions.

2. **SQLite persistence**
   - **Риск:** `data/` монтируется в Docker (`./data:/app/data`). При пересоздании контейнера без volume — потеря БД.
   - **Смягчение:** Использовать named volume или не удалять `./data` при `docker-compose down`.

3. **WEB_PASS обязателен**
   - **Риск:** Без `WEB_PASS` serve не стартует; в `.env.example` поле пустое.
   - **Смягчение:** Проверять наличие перед деплоем; README описывает требование.

### Высокие (P2)

4. **config.yaml при Docker**
   - **Риск:** `./config.yaml:/app/config.yaml:ro` — если файла нет, Docker создаёт каталог; загрузка конфига падает.
   - **Смягчение:** `config.yaml` есть в репо; копировать при первом деплое.

5. **Scheduler/queue при рестарте**
   - **Риск:** Pending tasks восстанавливаются через `requeue_startup_tasks`; при долгом downtime — накопление задач.
   - **Смягчение:** Логика есть; покрыта тестами; при падении worker’а running-задачи помечаются failed.

6. **Session secret в БД**
   - **Риск:** При восстановлении из бэкапа без БД — новый secret; старые cookies невалидны.
   - **Смягчение:** Ожидаемое поведение; пользователь перелогинивается.

7. **Web security**
   - **Риск:** HTTP по умолчанию; cookie без `Secure` на HTTP (тест `test_cookie_not_secure_on_http`).
   - **Смягчение:** За reverse proxy с HTTPS — `Secure` выставляется (тест `test_cookie_secure_on_https`).

### Средние (P3)

8. **Flood wait**
   - **Риск:** Все аккаунты в flood_wait — сбор блокируется.
   - **Смягчение:** Ротация по `max_flood_wait_sec`; можно добавить аккаунты.

9. **Backup**
   - **Риск:** Нет встроенного бэкапа SQLite.
   - **Смягчение:** Рекомендуется cron + `sqlite3 .backup` или файловый бэкап `data/`.

10. **Healthcheck**
    - **Риск:** Healthcheck проверяет только HTTP 200 `/health`; не проверяет scheduler/queue.
    - **Смягчение:** Базовая проверка работоспособности присутствует.

---

## 5. Pre-Deploy Runbook

### До запуска

- [ ] `cp .env.example .env` и заполнить:
  - [ ] `TG_API_ID`, `TG_API_HASH` (обязательно)
  - [ ] `WEB_PASS` (обязательно)
  - [ ] `SESSION_ENCRYPTION_KEY` (рекомендуется до добавления аккаунтов)
  - [ ] `LLM_API_KEY` (если нужен AI-поиск)
- [ ] Убедиться, что `config.yaml` присутствует (в репо по умолчанию есть).
- [ ] Для Docker: проверить, что `./data` и `./config.yaml` доступны (для volume mount).
- [ ] Запустить тесты: `pytest tests/ -v`.
- [ ] Проверить линтер: `ruff check src/ tests/`.

### Запуск

**Локально:**
```bash
pip install .
python -m src.main serve
# или с кастомным паролем: python -m src.main serve --web-pass yourpass
```

**Docker:**
```bash
docker-compose up -d
# Проверить логи: docker-compose logs -f
```

### После старта

- [ ] Открыть `http://localhost:8080` (или свой хост/порт).
- [ ] Пройти `/login`, ввести `WEB_PASS`.
- [ ] Проверить `/health`: `{"status":"healthy","db":true,...}`.
- [ ] Добавить Telegram-аккаунт через `/auth/login`.
- [ ] Проверить dashboard: аккаунты, каналы, статус scheduler’а.
- [ ] При использовании Docker: `docker inspect --format='{{.State.Health.Status}}' <container>`.

### Откат

- Остановить: `docker-compose down` (или Ctrl+C для локального).
- БД остаётся в `./data` при bind mount.
- При проблемах с сессиями: задать корректный `SESSION_ENCRYPTION_KEY` или очистить таблицу `accounts` (потеря авторизаций).

---

## 6. Рекомендуемые улучшения

### Быстрые и безопасные

1. **README/Docker**: добавить в README предупреждение, что `config.yaml` должен существовать на хосте для volume mount (если не используется дефолтный из репо).
2. **.env.example**: добавить комментарий, что `SESSION_ENCRYPTION_KEY` лучше задать до первого аккаунта.
3. **Healthcheck**: расширить `/health`, чтобы опционально возвращать статус scheduler (running/stopped) — без ломания контракта.

### Средний приоритет

4. **Backup**: добавить скрипт или инструкцию для бэкапа SQLite (например, `sqlite3 data/tg_search.db ".backup backup.db"`).
5. **Docker**: рассмотреть named volume для `data` вместо bind mount — проще перенос между хостами.
6. **Тесты**: добавить smoke-тест в Docker (например, в CI: build + run + curl health).

### Долгосрочные

7. **Rate limiting**: ограничение попыток входа по IP.
8. **Метрики**: Prometheus/health endpoint с метриками (очередь, scheduler, accounts).
9. **Миграции**: версионирование миграций (например, в отдельной таблице) вместо `PRAGMA table_info`.

---

*Аудит выполнен на основе анализа кодовой базы, конфигурации и тестов. Для production-развёртывания рекомендуется дополнительная проверка на стенде с реальными Telegram-аккаунтами.*
