from django.db import models
import uuid
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True

class Genre(UUIDMixin, TimeStampedMixin):
    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField('Название', max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField('Описание', blank=True, null=True) # Добавил null=True для консистентности с FilmWork

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

def __str__(self):
        return self.name

class FilmWork(UUIDMixin, TimeStampedMixin):
    class FilmWorkType(models.TextChoices):
        MOVIE = 'movie', 'Фильм'
        TV_SHOW = 'tv_show', 'ТВ-шоу'

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField('Описание', blank=True, null=True)
    creation_date = models.DateField('Дата создания фильма', blank=True, null=True)
    rating = models.FloatField('Рейтинг', blank=True, null=True,
                               validators=[MinValueValidator(0), MaxValueValidator(100)])
    type = models.CharField(
        'Тип',
        max_length=10, # Должно быть достаточно для 'movie' и 'tv_show'
        choices=FilmWorkType.choices,
        default=FilmWorkType.MOVIE
    )

    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField('Person', through='PersonFilmWork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведения'

    def __str__(self):
        return self.title

class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        # Для уникальности пары film_work-genre
        unique_together = ('film_work', 'genre')
        verbose_name = 'Жанр кинопроизведения'
        verbose_name_plural = 'Жанры кинопроизведения'

class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField('Полное имя', max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = 'Персона'
        verbose_name_plural = 'Персоны'

    def __str__(self):
        return self.full_name


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('Роль', null=True) # Сделаем null=True на случай, если роль не всегда обязательна
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        # Убедимся, что одна и та же персона не может иметь одну и ту же роль в одном фильме дважды
        unique_together = ('film_work', 'person', 'role')
        verbose_name = 'Участник кинопроизведения'
        verbose_name_plural = 'Участники кинопроизведения'

