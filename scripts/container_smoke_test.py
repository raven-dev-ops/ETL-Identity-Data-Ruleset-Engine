from __future__ import annotations

import argparse
import http.client
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "deploy" / "compose.yaml"


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=check,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )


def _wait_for_health(port: int, reader_api_key: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/healthz",
        headers={"X-API-Key": reader_api_key},
    )
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, http.client.RemoteDisconnected, TimeoutError):
            time.sleep(1)
            continue
    raise RuntimeError(f"Timed out waiting for containerized service health on port {port}")


def _write_env_file(
    path: Path,
    *,
    image_tag: str,
    runtime_root: Path,
    service_port: int,
    reader_api_key: str,
    operator_api_key: str,
) -> None:
    path.write_text(
        "\n".join(
            [
                f"ETL_IDENTITY_IMAGE={image_tag}",
                f"ETL_IDENTITY_RUNTIME_ROOT={runtime_root.as_posix()}",
                f"ETL_IDENTITY_CONFIG_ROOT={(REPO_ROOT / 'config').resolve().as_posix()}",
                "ETL_IDENTITY_STATE_DB=/runtime/state/pipeline_state.sqlite",
                f"ETL_IDENTITY_SERVICE_PORT={service_port}",
                f"ETL_IDENTITY_SERVICE_READER_API_KEY={reader_api_key}",
                f"ETL_IDENTITY_SERVICE_OPERATOR_API_KEY={operator_api_key}",
                "ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY=disabled",
                "ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY=disabled",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _ensure_runtime_root_permissions(runtime_root: Path) -> None:
    for directory in (
        runtime_root,
        runtime_root / "state",
        runtime_root / "output",
        runtime_root / "published",
    ):
        directory.mkdir(parents=True, exist_ok=True)
        os.chmod(directory, 0o777)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-tag", default="etl-identity-engine:container-smoke")
    parser.add_argument("--service-port", default=18080, type=int)
    parser.add_argument("--reader-api-key", default="reader-secret")
    parser.add_argument("--operator-api-key", default="operator-secret")
    args = parser.parse_args(argv)

    runtime_root = Path(tempfile.mkdtemp(prefix="etl-identity-engine-container-smoke-"))
    _ensure_runtime_root_permissions(runtime_root)
    env_file = runtime_root / "container.env"
    service_name = "identity-service"
    try:
        _write_env_file(
            env_file,
            image_tag=args.image_tag,
            runtime_root=runtime_root,
            service_port=args.service_port,
            reader_api_key=args.reader_api_key,
            operator_api_key=args.operator_api_key,
        )

        print(f"building image {args.image_tag}")
        _run(["docker", "build", "-t", args.image_tag, "."])

        print("validating CLI entrypoint")
        _run(["docker", "run", "--rm", args.image_tag, "--help"])

        print("validating compose configuration")
        _run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "--env-file",
                str(env_file),
                "config",
            ]
        )

        print("running batch container")
        _run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "--env-file",
                str(env_file),
                "run",
                "--rm",
                "identity-batch",
            ]
        )

        state_db = runtime_root / "state" / "pipeline_state.sqlite"
        if not state_db.exists():
            raise RuntimeError(f"Expected persisted state database was not created: {state_db}")

        print("starting service container")
        _run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "--env-file",
                str(env_file),
                "up",
                "-d",
                service_name,
            ]
        )

        _wait_for_health(
            args.service_port,
            reader_api_key=args.reader_api_key,
            timeout_seconds=60,
        )
        metrics_request = urllib.request.Request(
            f"http://127.0.0.1:{args.service_port}/api/v1/metrics",
            headers={"X-API-Key": args.reader_api_key},
        )
        with urllib.request.urlopen(metrics_request, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("Containerized service metrics endpoint did not return 200")
        print("container smoke test passed")
        return 0
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout)
        sys.stderr.write(exc.stderr)
        raise
    except Exception:
        logs = _run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "--env-file",
                str(env_file),
                "logs",
                service_name,
            ],
            check=False,
        )
        sys.stderr.write(logs.stdout)
        sys.stderr.write(logs.stderr)
        raise
    finally:
        try:
            _run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(COMPOSE_FILE),
                    "--env-file",
                    str(env_file),
                    "down",
                    "--remove-orphans",
                    "--volumes",
                ],
                check=False,
            )
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
