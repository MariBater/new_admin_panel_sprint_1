from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import FilmWork


class MoviesApiMixin:
    model = FilmWork
    http_method_names = ['get']

    def get_queryset(self):
        return FilmWork.objects.all()

    def _get_annotated_queryset(self, queryset):
        return queryset.prefetch_related('genres', 'persons').annotate(
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
    paginate_by = 50

    def get_queryset(self):
        queryset = super().get_queryset()

        title = self.request.GET.get('title')
        if title:
            queryset = queryset.filter(title__icontains=title)

        genre = self.request.GET.get('genre')
        if genre:
            queryset = queryset.filter(genres__name__icontains=genre)

        return self._get_annotated_queryset(queryset)

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super().get_context_data(**kwargs)
        page = context['page_obj']
        paginator = context['paginator']
        return {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': list(context['object_list']),
        }


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    def get_queryset(self):
        queryset = super().get_queryset()
        return self._get_annotated_queryset(queryset)

    def get_context_data(self, **kwargs):
        return kwargs['object']