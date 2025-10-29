"""
URL patterns for dashboard app.
"""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard_home, name='dashboard'),
    path('files/', views.files_list, name='files_list'),
    path('jobs/', views.jobs_list, name='jobs_list'),
    path('meta-jobs/', views.meta_jobs_list, name='meta_jobs_list'),
    path('methods/', views.methods_list, name='methods_list'),
    path('slurm/', views.slurm_queue, name='slurm_queue'),
    path('environment/', views.environment_info, name='environment_info'),
    path('logs/', views.logs_viewer, name='logs_viewer'),
]

