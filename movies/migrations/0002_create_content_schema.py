# movies/migrations/0002_create_content_schema.py

from django.db import migrations

class Migration(migrations.Migration):

    # Запускаем эту миграцию ПЕРЕД первой (0001_initial)
    # Для этого убираем ее из зависимостей и добавляем в run_before
    dependencies = []

    run_before = [
        ('movies', '0001_initial'),
    ]

    operations = [
        # Создаем схему 'content', если она еще не существует
        migrations.RunSQL('CREATE SCHEMA IF NOT EXISTS content;'),
    ]

