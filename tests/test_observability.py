from __future__ import annotations

import json

from etl_identity_engine.observability import emit_structured_log


def test_emit_structured_log_redacts_free_text_and_auth_material(capsys) -> None:
    emit_structured_log(
        "security_redaction_check",
        notes="Approved John Doe after address verification",
        authorization="Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
        nested={
            "token": "eyJhbGciOiJIUzI1NiJ9.payload.signature",
            "error": (
                "Authorization failed for Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature "
                "against postgresql://svc_user:super-secret-password@db.internal/identity"
            ),
        },
    )

    payload = json.loads(capsys.readouterr().err)
    assert payload["event"] == "security_redaction_check"
    assert payload["notes"].startswith("[REDACTED free_text len=")
    assert payload["authorization"] == "[REDACTED auth_material]"
    assert payload["nested"]["token"] == "[REDACTED auth_material]"
    assert "super-secret-password" not in payload["nested"]["error"]
    assert "payload.signature" not in payload["nested"]["error"]
    assert "Bearer [REDACTED]" in payload["nested"]["error"]

