from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0001_initial'),
    ]

    operations = [
        # This operation creates the 'content' schema if it doesn't exist.
        migrations.RunSQL("CREATE SCHEMA IF NOT EXISTS content;"),
    ]