from __future__ import annotations

import os
import sys
from pathlib import Path

from etl_identity_engine.demo_shell.bootstrap import configure_demo_shell_environment


def main() -> None:
    output_dir = Path(os.environ.get("PUBLIC_SAFETY_DEMO_BASE_DIR", "dist/public-safety-demo-django"))
    configure_demo_shell_environment(output_dir=output_dir)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "etl_identity_engine.demo_shell.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
