from django.test import TestCase
from django.utils import timezone
from movies.models import Genre, FilmWork  
import uuid



class DataConsistencyTest(TestCase):

    def test_genre_filmwork_consistency(self):
        """Проверяем количество и содержимое записей в Genre и FilmWork."""

        #  Создадим несколько объектов Genre
        genre1 = Genre.objects.create(id=uuid.uuid4(), name='Комедия', description='Смешные фильмы', created=timezone.now(), modified=timezone.now())
        genre2 = Genre.objects.create(id=uuid.uuid4(), name='Драма', description='Серьезные фильмы', created=timezone.now(), modified=timezone.now())

        #  Создадим несколько объектов FilmWork и свяжем с жанрами
        film1 = FilmWork.objects.create(id=uuid.uuid4(), title='Фильм 1', type='movie', created=timezone.now(), modified=timezone.now())
        film1.genres.add(genre1)
        film2 = FilmWork.objects.create(id=uuid.uuid4(), title='Фильм 2', type='movie', created=timezone.now(), modified=timezone.now())
        film2.genres.add(genre2)

        #  Проверяем количество созданных объектов
        self.assertEqual(Genre.objects.count(), 2)
        self.assertEqual(FilmWork.objects.count(), 2)

        #  Проверяем содержимое (пример: проверяем название одного из жанров)
        self.assertEqual(Genre.objects.get(name='Комедия').description, 'Смешные фильмы')

        # Пример проверки связи: подсчитываем фильмы в жанре "Драма"
        drama_films_count = FilmWork.objects.filter(genres__name='Драма').count()
        self.assertEqual(drama_films_count, 1)

        #  Дополнительные проверки содержимого и связей (по аналогии)
        #  Например, проверьте, что у film1 есть жанр "Комедия", а у film2 - "Драма"
        self.assertTrue(film1.genres.filter(name='Комедия').exists())
        self.assertFalse(film1.genres.filter(name='Драма').exists())

        self.assertTrue(film2.genres.filter(name='Драма').exists())
        self.assertFalse(film2.genres.filter(name='Комедия').exists())