import sqlite3
import os

# Путь к файлу базы данных SQLite
# Скрипт ожидает, что db.sqlite находится в той же директории, что и сам скрипт
db_filename = 'db.sqlite'
db_path = r'd:\work\new_admin_panel_sprint_1\sqlite_to_postgres\db.sqlite'

print(f"Проверка файла базы данных: {db_path}")

if not os.path.exists(db_path):
    print(f"Файл '{db_filename}' не найден по пути '{db_path}'. Убедитесь, что файл существует.")
else:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Запрос для получения списка всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if tables:
            print("\nНайденные таблицы в базе данных:")
            for table in tables:
                print(f"- {table[0]}")

                # Опционально: вывести структуру каждой таблицы
                print(f"  Структура таблицы '{table[0]}':")
                cursor.execute(f"PRAGMA table_info('{table[0]}');")
                columns = cursor.fetchall()
                for column in columns:
                    # cid, name, type, notnull, dflt_value, pk
                    print(f"    - {column[1]} ({column[2]})")
                print("-" * 20)

        else:
            print(f"\nВ базе данных '{db_path}' таблицы не найдены. Файл может быть пустым или не содержать таблиц.")

    except sqlite3.Error as e:
        print(f"Ошибка SQLite: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
