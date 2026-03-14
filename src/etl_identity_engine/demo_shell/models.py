from __future__ import annotations

from django.db import models


class DemoRun(models.Model):
    bundle_name = models.CharField(max_length=255)
    bundle_path = models.TextField()
    bundle_root = models.TextField()
    version = models.CharField(max_length=64)
    profile = models.CharField(max_length=32)
    seed = models.IntegerField()
    formats = models.JSONField(default=list)
    generated_at_utc = models.CharField(max_length=64, blank=True)
    source_commit = models.CharField(max_length=64, blank=True)
    artifact_paths = models.JSONField(default=list)
    summary = models.JSONField(default=dict)
    top_golden_people_by_activity = models.JSONField(default=list)
    incident_count = models.IntegerField(default=0)
    incident_person_link_count = models.IntegerField(default=0)
    cad_incident_count = models.IntegerField(default=0)
    rms_incident_count = models.IntegerField(default=0)
    resolved_link_count = models.IntegerField(default=0)
    unresolved_link_count = models.IntegerField(default=0)
    linked_golden_person_count = models.IntegerField(default=0)
    cross_system_golden_person_count = models.IntegerField(default=0)
    loaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-loaded_at", "-id"]

    def __str__(self) -> str:
        return f"{self.bundle_name} ({self.profile}, seed={self.seed})"


class DemoScenario(models.Model):
    demo_run = models.ForeignKey(DemoRun, on_delete=models.CASCADE, related_name="scenarios")
    scenario_id = models.SlugField(max_length=64)
    title = models.CharField(max_length=255)
    golden_id = models.CharField(max_length=128, blank=True)
    golden_name = models.CharField(max_length=255, blank=True)
    narrative = models.TextField(blank=True)
    cad_incident_count = models.IntegerField(default=0)
    rms_incident_count = models.IntegerField(default=0)
    total_incident_count = models.IntegerField(default=0)
    latest_incident_at = models.CharField(max_length=64, blank=True)

    class Meta:
        ordering = ["id"]
        unique_together = ("demo_run", "scenario_id")

    def __str__(self) -> str:
        return self.title


class GoldenPersonActivity(models.Model):
    demo_run = models.ForeignKey(DemoRun, on_delete=models.CASCADE, related_name="golden_activity_rows")
    golden_id = models.CharField(max_length=128, db_index=True)
    cluster_id = models.CharField(max_length=128, blank=True)
    person_entity_id = models.CharField(max_length=128, blank=True)
    golden_first_name = models.CharField(max_length=255, blank=True)
    golden_last_name = models.CharField(max_length=255, blank=True)
    cad_incident_count = models.IntegerField(default=0)
    rms_incident_count = models.IntegerField(default=0)
    total_incident_count = models.IntegerField(default=0)
    linked_source_record_count = models.IntegerField(default=0)
    roles = models.TextField(blank=True)
    latest_incident_at = models.CharField(max_length=64, blank=True)

    class Meta:
        ordering = ["-total_incident_count", "golden_last_name", "golden_first_name", "golden_id"]

    @property
    def golden_name(self) -> str:
        return " ".join(part for part in (self.golden_first_name, self.golden_last_name) if part).strip()

    @property
    def master_person_id(self) -> str:
        return self.golden_id

    @property
    def agency_footprint(self) -> str:
        if self.cad_incident_count and self.rms_incident_count:
            return "CAD + RMS"
        if self.cad_incident_count:
            return "CAD only"
        if self.rms_incident_count:
            return "RMS only"
        return "No linked incidents"

    def __str__(self) -> str:
        return f"{self.golden_id}: {self.golden_name}"


class IncidentIdentity(models.Model):
    demo_run = models.ForeignKey(DemoRun, on_delete=models.CASCADE, related_name="incident_identity_rows")
    incident_id = models.CharField(max_length=128, db_index=True)
    incident_source_system = models.CharField(max_length=32, blank=True)
    occurred_at = models.CharField(max_length=64, blank=True)
    incident_location = models.TextField(blank=True)
    incident_city = models.CharField(max_length=128, blank=True)
    incident_state = models.CharField(max_length=32, blank=True)
    incident_role = models.CharField(max_length=128, blank=True)
    person_entity_id = models.CharField(max_length=128, blank=True)
    source_record_id = models.CharField(max_length=128, blank=True)
    person_source_system = models.CharField(max_length=32, blank=True)
    golden_id = models.CharField(max_length=128, blank=True, db_index=True)
    cluster_id = models.CharField(max_length=128, blank=True)
    golden_first_name = models.CharField(max_length=255, blank=True)
    golden_last_name = models.CharField(max_length=255, blank=True)
    golden_dob = models.CharField(max_length=64, blank=True)
    golden_address = models.TextField(blank=True)
    golden_phone = models.CharField(max_length=64, blank=True)

    class Meta:
        ordering = ["-occurred_at", "incident_id", "golden_id", "id"]

    @property
    def golden_name(self) -> str:
        return " ".join(part for part in (self.golden_first_name, self.golden_last_name) if part).strip()

    @property
    def system_display(self) -> str:
        source_system = (self.incident_source_system or "").strip().lower()
        if source_system == "cad":
            return "CAD"
        if source_system == "rms":
            return "RMS"
        return source_system.upper()

    @property
    def system_record_type(self) -> str:
        source_system = (self.incident_source_system or "").strip().lower()
        if source_system == "cad":
            return "CAD Call"
        if source_system == "rms":
            return "RMS Report"
        return "Incident"

    @property
    def incident_reference_label(self) -> str:
        source_system = (self.incident_source_system or "").strip().lower()
        if source_system == "cad":
            return "Call Number"
        if source_system == "rms":
            return "Report Number"
        return "Incident Number"

    @property
    def incident_reference_value(self) -> str:
        return self.incident_id

    @property
    def agency_name(self) -> str:
        city = (self.incident_city or "").strip()
        state = (self.incident_state or "").strip()
        location_prefix = " ".join(part for part in (city, state) if part).strip()
        source_system = (self.incident_source_system or "").strip().lower()
        if source_system == "cad":
            suffix = "Regional Dispatch"
        elif source_system == "rms":
            suffix = "Police Records"
        else:
            suffix = "Public Safety"
        return f"{location_prefix} {suffix}".strip()

    @property
    def master_person_id(self) -> str:
        return self.golden_id or self.cluster_id

    def __str__(self) -> str:
        return f"{self.incident_id} -> {self.golden_id or 'unresolved'}"
