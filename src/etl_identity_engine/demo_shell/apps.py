from django.apps import AppConfig


class DemoShellConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "etl_identity_engine.demo_shell"
    verbose_name = "Public Safety Demo Shell"
