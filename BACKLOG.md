# Shadowbroker Backlog

Items to pick up once the modular branch is merged to main.

---

## Frontend

### Ship layer toggles — split military/cargo
Currently `ships_important` bundles military, carrier, tanker, cargo together.
Split into:
- `ships_military` — `military_vessel` + `carrier`
- `ships_cargo` — `cargo` + `tanker`
- `ships_passenger` — `passenger` (exists)
- `ships_civilian` — `yacht` + `unknown` (exists)
Change is in `MaplibreViewer.tsx` filter logic + `activeLayers` keys + filter panel UI.

---

## Backend

### Connection manager
Each module rolls its own timeout/retry. A shared connection manager with
per-host rate limiting, backoff, and circuit breaking would clean this up.
`httpx` async client is the natural upgrade path.

### APScheduler log levels
Scheduler firing logs (`executed successfully`, `Running job`) currently at INFO.
Defer to DEBUG once things are stable. Already demoted `apscheduler.executors.default`
in `main.py` — verify it's working as expected.

### LiveUAMap
Cloudscraper replacement working but liveuamap.com unreachable from this network.
Validate once network access is available.

### readsb-feeder branch
`feat/readsb-feeder` — local tar1090/readsb supplemental ADS-B source.
One clean commit (`43bad1d`). Rewrite for modular format when modular lands on main.

### aircraft-enrichment branch
`feat/aircraft-enrichment` — `/api/aircraft/{icao24}` endpoint + POTUS fleet tracker.
Heavy `data_fetcher.py` changes, needs porting to modular layout.
Key files: `services/aircraft_enrichment.py` (new endpoint), `plane_alert_db.json` update.
