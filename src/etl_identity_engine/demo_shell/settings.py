from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(os.environ.get("PUBLIC_SAFETY_DEMO_BASE_DIR", Path.cwd())).resolve()
SECRET_KEY = os.environ.get("PUBLIC_SAFETY_DEMO_SECRET_KEY", "public-safety-demo-only")
DEBUG = os.environ.get("PUBLIC_SAFETY_DEMO_DEBUG", "1") == "1"
ALLOWED_HOSTS = [
    host.strip()
    for host in os.environ.get(
        "PUBLIC_SAFETY_DEMO_ALLOWED_HOSTS",
        "127.0.0.1,localhost,testserver",
    ).split(",")
    if host.strip()
]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "etl_identity_engine.demo_shell",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "etl_identity_engine.demo_shell.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {"context_processors": []},
    },
]

WSGI_APPLICATION = "etl_identity_engine.demo_shell.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.environ.get("PUBLIC_SAFETY_DEMO_DB", str(BASE_DIR / "db.sqlite3")),
    }
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True
STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

PUBLIC_SAFETY_DEMO_BUNDLE_ROOT = Path(
    os.environ.get("PUBLIC_SAFETY_DEMO_BUNDLE_ROOT", str(BASE_DIR / "bundle"))
).resolve()
