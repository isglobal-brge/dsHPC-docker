"""
URL patterns for dashboard app.
"""
from django.urls import path
from . import views
from . import api_views

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('', views.dashboard_home, name='dashboard'),
    path('api/container-status/', views.container_status, name='container_status'),
    path('api/snapshot-timestamp/', api_views.snapshot_timestamp, name='snapshot_timestamp'),
    path('files/', views.files_list, name='files_list'),
    path('jobs/', views.jobs_list, name='jobs_list'),
    path('meta-jobs/', views.meta_jobs_list, name='meta_jobs_list'),
    path('methods/', views.methods_list, name='methods_list'),
    path('slurm/', views.slurm_queue, name='slurm_queue'),
    path('slurm/job-logs/<str:slurm_id>/', views.slurm_job_logs, name='slurm_job_logs'),
    path('environment/', views.environment_info, name='environment_info'),
    path('logs/', views.logs_viewer, name='logs_viewer'),
]

