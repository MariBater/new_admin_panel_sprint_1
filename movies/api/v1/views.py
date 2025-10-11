from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import FilmWork


class MoviesApiMixin:
    model = FilmWork
    http_method_names = ['get']

    def _get_annotated_queryset(self):
        return FilmWork.objects.prefetch_related('genres', 'persons').annotate(
            genres=ArrayAgg('genres__name', distinct=True),
            actors=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='actor')
            ),
            directors=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='director')
            ),
            writers=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role='writer')
            ),
        ).values(
            'id', 'title', 'description', 'creation_date', 'rating', 'type',
            'genres', 'actors', 'directors', 'writers'
        )

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesApi(MoviesApiMixin, BaseListView):

    def get_queryset(self):
        return self._get_annotated_queryset()

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        return {'results': list(queryset)}


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    def get_queryset(self):
        queryset = super().get_queryset()
        return self._get_annotated_queryset(queryset)

    def get_context_data(self, **kwargs):
        return kwargs['object']