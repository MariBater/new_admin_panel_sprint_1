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
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content.genre"
        verbose_name = _('Жанр')
        verbose_name_plural = _('Жанры')

    def __str__(self):
        return self.name

class FilmWork(UUIDMixin, TimeStampedMixin):
    class FilmWorkType(models.TextChoices):
        MOVIE = 'movie', _('Movie')
        TV_SHOW = 'tv_show', _('TV Show')

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, default='')
    creation_date = models.DateField(_('creation_date'), blank=True, null=True)
    rating = models.FloatField(
        _('rating'),
        blank=True,
        null=True,
        validators=[MinValueValidator(0), MaxValueValidator(10)]
    )
    type = models.CharField(
        _('type'),
        max_length=7,
        choices=FilmWorkType.choices,
    )
    genres = models.ManyToManyField(Genre, through='GenreFilmWork', verbose_name=_('genres'))
    persons = models.ManyToManyField('Person', through='PersonFilmWork', verbose_name=_('persons'))
    

    class Meta:
        db_table = "content.film_work"
        verbose_name = _('film work')
        verbose_name_plural = _('film works')

    def __str__(self):
        return self.title

   
class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE, verbose_name=_('film work'))
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE, verbose_name=_('Жанр'))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content.genre_film_work"
        verbose_name = _('genre of film work')
        unique_together = ('film_work', 'genre')
        verbose_name_plural = _('genres of film works')

class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full name'), max_length=255)

    class Meta:
        db_table = "content.person"
        verbose_name = _('Персона')
        verbose_name_plural = _('Персоны')

    def __str__(self):
        return self.full_name

class PersonFilmWork(UUIDMixin):
    class PersonRole(models.TextChoices):
        ACTOR = 'actor', _('Actor')
        DIRECTOR = 'director', _('Director')
        WRITER = 'writer', _('Writer')

    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE, verbose_name=_('film work'))
    person = models.ForeignKey('Person', on_delete=models.CASCADE, verbose_name=_('Персона'))
    role = models.CharField(_('Роль'), max_length=10, choices=PersonRole.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content.person_film_work"
        verbose_name = _('film work participant')
        verbose_name_plural = _('film work participants')
        unique_together = ('film_work', 'person', 'role')
