from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork




@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created', 'modified')
    search_fields = ('name', 'description')

class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork   
    extra = 1 # Количество пустых форм для добавления

class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    extra = 1
@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    list_display = ('title', 'description', 'creation_date', 'rating', 'type', 'created', 'modified')
    list_filter = ('type',)
    search_fields = ('title', 'description')
    inlines = (GenreFilmWorkInline,PersonFilmWorkInline)

@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'created', 'modified')
    search_fields = ('full_name',)

