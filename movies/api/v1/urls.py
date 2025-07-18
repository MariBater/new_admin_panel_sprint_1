from django.urls import path

from movies.api.v1 import views

urlpatterns = [
    path('movies/<uuid:pk>/', views.MoviesDetailApi.as_view()),
    path('movies/', views.MoviesApi.as_view()),
]
