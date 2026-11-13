# Garmin pipeline — DEV 1.1 preparation

Status: **prepared only; not activated and not scheduled**.

## Production baseline

- Production branch: `main`
- Production workflow: `Garmin to MotherDock Prod`
- Production database: `garmin_github`
- Production refresh: rolling latest 30 days; historical records must remain untouched.
- The production workflow is manually dispatchable and scheduled only from the default branch.
- Do not change production workflow, production secrets, or production database while developing 1.1.

## Version 1.1 backlog

1. **Gear / shoes**
   - Resolve Garmin gear lookup (the current endpoint returned HTTP 404).
   - Map activity gear IDs to historical gear records where available.
   - Preserve unknown/unmapped IDs; never guess a shoe.

2. **VO2 max**
   - Read the dedicated Garmin VO2 max sources, not only activity summaries.
   - Preserve measurement date, value, source, and activity association where Garmin supplies it.
   - Validate historical coverage against the Garmin export.

3. **Stress and steps**
   - Investigate the HTTP 400 responses for October 2026.
   - Determine supported date windows and endpoint parameters.
   - Keep unavailable periods explicitly distinguishable from true zero values.

## DEV isolation rules

- Develop on branch `dev-1.1`; do not commit experimental code to `main`.
- Use a separate DEV database and DEV-only credentials before any test writes.
- Never point DEV code at `garmin_github`.
- Do not add a scheduled trigger or enable an ingestion workflow for DEV until explicitly approved.
- Test with bounded date ranges and compare source payloads, row counts, null rates, and historical preservation.
- Promote to production only after validation and an explicit release decision.

## Readiness checklist before DEV ingestion is enabled

- [ ] Confirm the exact DEV database and its ownership/access.
- [ ] Configure separate DEV Garmin and MotherDuck secrets.
- [ ] Add a manual-only DEV workflow with an explicit DEV database target.
- [ ] Add safeguards that reject production database names in DEV runs.
- [ ] Add tests for gear mapping, VO2 max history, and stress/steps availability.
- [ ] Validate a small date window before expanding the test period.

No ingestion is started by this preparation.
