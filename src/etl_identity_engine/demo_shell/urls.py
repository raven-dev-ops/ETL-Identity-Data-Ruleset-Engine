from __future__ import annotations

from django.urls import path

from etl_identity_engine.demo_shell import views


urlpatterns = [
    path("", views.index, name="demo-shell-index"),
    path("scenarios/<slug:scenario_id>/", views.scenario_detail, name="demo-shell-scenario"),
    path("golden/<str:golden_id>/", views.golden_detail, name="demo-shell-golden"),
    path("artifacts/<path:relative_path>", views.artifact_file, name="demo-shell-artifact"),
]
