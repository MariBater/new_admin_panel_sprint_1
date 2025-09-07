from django.contrib import admin
from .models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


class GenreFilmWorkInline(admin.TabularInline):
    """
    Позволяет редактировать жанры на странице кинопроизведения.
    """
    model = GenreFilmWork
    autocomplete_fields = ('genre',)
    extra = 0


class PersonFilmWorkInline(admin.TabularInline):
    """
    Позволяет редактировать участников на странице кинопроизведения.
    """
    model = PersonFilmWork
    autocomplete_fields = ('person',)
    extra = 0


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name',)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    # Подключаем инлайны для управления жанрами и участниками
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)

    # Поля, которые будут отображаться в списке кинопроизведений
    list_display = ('title', 'type', 'creation_date', 'rating', 'created', 'modified')

    # Фильтры для удобной навигации
    list_filter = ('type',)

    # Поля, по которым будет работать поиск
    search_fields = ('title', 'description', 'id')
