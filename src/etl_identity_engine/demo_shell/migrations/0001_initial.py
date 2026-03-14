from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DemoRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("bundle_name", models.CharField(max_length=255)),
                ("bundle_path", models.TextField()),
                ("bundle_root", models.TextField()),
                ("version", models.CharField(max_length=64)),
                ("profile", models.CharField(max_length=32)),
                ("seed", models.IntegerField()),
                ("formats", models.JSONField(default=list)),
                ("generated_at_utc", models.CharField(blank=True, max_length=64)),
                ("source_commit", models.CharField(blank=True, max_length=64)),
                ("artifact_paths", models.JSONField(default=list)),
                ("summary", models.JSONField(default=dict)),
                ("top_golden_people_by_activity", models.JSONField(default=list)),
                ("incident_count", models.IntegerField(default=0)),
                ("incident_person_link_count", models.IntegerField(default=0)),
                ("cad_incident_count", models.IntegerField(default=0)),
                ("rms_incident_count", models.IntegerField(default=0)),
                ("resolved_link_count", models.IntegerField(default=0)),
                ("unresolved_link_count", models.IntegerField(default=0)),
                ("linked_golden_person_count", models.IntegerField(default=0)),
                ("cross_system_golden_person_count", models.IntegerField(default=0)),
                ("loaded_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"ordering": ["-loaded_at", "-id"]},
        ),
        migrations.CreateModel(
            name="DemoScenario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("scenario_id", models.SlugField(max_length=64)),
                ("title", models.CharField(max_length=255)),
                ("golden_id", models.CharField(blank=True, max_length=128)),
                ("golden_name", models.CharField(blank=True, max_length=255)),
                ("narrative", models.TextField(blank=True)),
                ("cad_incident_count", models.IntegerField(default=0)),
                ("rms_incident_count", models.IntegerField(default=0)),
                ("total_incident_count", models.IntegerField(default=0)),
                ("latest_incident_at", models.CharField(blank=True, max_length=64)),
                (
                    "demo_run",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="scenarios", to="demo_shell.demorun"),
                ),
            ],
            options={"ordering": ["id"], "unique_together": {("demo_run", "scenario_id")}},
        ),
        migrations.CreateModel(
            name="GoldenPersonActivity",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("golden_id", models.CharField(db_index=True, max_length=128)),
                ("cluster_id", models.CharField(blank=True, max_length=128)),
                ("person_entity_id", models.CharField(blank=True, max_length=128)),
                ("golden_first_name", models.CharField(blank=True, max_length=255)),
                ("golden_last_name", models.CharField(blank=True, max_length=255)),
                ("cad_incident_count", models.IntegerField(default=0)),
                ("rms_incident_count", models.IntegerField(default=0)),
                ("total_incident_count", models.IntegerField(default=0)),
                ("linked_source_record_count", models.IntegerField(default=0)),
                ("roles", models.TextField(blank=True)),
                ("latest_incident_at", models.CharField(blank=True, max_length=64)),
                (
                    "demo_run",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="golden_activity_rows",
                        to="demo_shell.demorun",
                    ),
                ),
            ],
            options={"ordering": ["-total_incident_count", "golden_last_name", "golden_first_name", "golden_id"]},
        ),
        migrations.CreateModel(
            name="IncidentIdentity",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("incident_id", models.CharField(db_index=True, max_length=128)),
                ("incident_source_system", models.CharField(blank=True, max_length=32)),
                ("occurred_at", models.CharField(blank=True, max_length=64)),
                ("incident_location", models.TextField(blank=True)),
                ("incident_city", models.CharField(blank=True, max_length=128)),
                ("incident_state", models.CharField(blank=True, max_length=32)),
                ("incident_role", models.CharField(blank=True, max_length=128)),
                ("person_entity_id", models.CharField(blank=True, max_length=128)),
                ("source_record_id", models.CharField(blank=True, max_length=128)),
                ("person_source_system", models.CharField(blank=True, max_length=32)),
                ("golden_id", models.CharField(blank=True, db_index=True, max_length=128)),
                ("cluster_id", models.CharField(blank=True, max_length=128)),
                ("golden_first_name", models.CharField(blank=True, max_length=255)),
                ("golden_last_name", models.CharField(blank=True, max_length=255)),
                ("golden_dob", models.CharField(blank=True, max_length=64)),
                ("golden_address", models.TextField(blank=True)),
                ("golden_phone", models.CharField(blank=True, max_length=64)),
                (
                    "demo_run",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="incident_identity_rows",
                        to="demo_shell.demorun",
                    ),
                ),
            ],
            options={"ordering": ["-occurred_at", "incident_id", "golden_id", "id"]},
        ),
    ]
