# Email Security Analytics Pipeline

A modular Python pipeline for ingesting raw email artifacts, extracting indicators, enriching with threat intelligence, classifying phishing/malware campaigns, and producing analyst‑ready reports and alerts.

## Features
- MIME/EML ingestion with header, body, and attachment parsing for large batches of messages.
- Indicator extraction: URLs, domains, IPs, hashes, senders, and DKIM/SPF/DMARC signals.
- Threat‑intelligence enrichment via pluggable providers (HTTP API adapters, local reputation lists).
- Rule and ML‑based detection for phishing, BEC, malware‑linked URLs, and spoofing patterns.
- Config‑driven pipeline stages to enable or disable modules without code changes.
- Alerting and reporting to JSON/CSV with optional webhook or email notifications.

## Architecture
- **Ingestion:** Reads `.eml`/`.msg` or raw RFC822 from folders or stdin and normalizes to structured records.
- **Parsing & Extraction:** Splits headers, body, and attachments; extracts indicators and email‑auth results.
- **Enrichment:** Queries threat‑intelligence sources and caches repeated IOCs.
- **Detection:** Applies signature rules plus an optional classifier; assigns severity and categories.
- **Output:** Writes events, IOCs, and alerts to `out/` as JSON/CSV; can forward to a webhook.

## Getting Started

### Prerequisites
- Python 3.10+ and pip
- Optional: virtualenv or venv for isolated installs

### Setup
git clone https://github.com/PrathamBhavsar2112/email-security-analytics-pipeline.git
cd email-security-analytics-pipeline
python -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate
pip install -r requirements.txt

text

Place sample `.eml` files under `data/input/` and run the pipeline.

## Configuration
`config.yaml` controls input paths, enabled modules, enrichment providers, thresholds, and output locations.

Secrets (API keys) should be provided via environment variables or a `.env` file ignored by version control.

input_dir: data/input/
output_dir: out/
enrichment:
urlscan:
enabled: true
api_key: ${URLSCAN_API_KEY}
virustotal:
enabled: false
detection:
ruleset: rules/phishing.yml
min_score: 0.6

text

## Usage

Process a directory:
python -m pipeline.run --config config.yaml --input data/input --output out

text

Process a single file:
python -m pipeline.run --file data/input/sample.eml

text

Enable debug logs:
python -m pipeline.run --config config.yaml --log-level DEBUG

text

## Outputs
- `out/events.jsonl` — normalized per‑message events
- `out/iocs.csv` — aggregated indicators (URL, domain, IP, hash, first_seen, sources)
- `out/alerts.json` — detections with severity, rule/model hits, and evidence excerpts

## Detection Logic
- **Rules:** YAML rules matching header anomalies, suspicious TLDs, look‑alike senders, and URL patterns
- **ML (optional):** Text/URL features for a binary phishing score
- **Email Auth:** DKIM/SPF/DMARC parsing to flag spoofing and alignment failures

## Extending
- Add a new enrichment provider by implementing `EnricherBase` and registering it in `config.yaml`.
- Add detection rules in `rules/*.yml`; each rule defines conditions, score, and tags.
- Implement new output sinks by subclassing `WriterBase` (e.g., SIEM webhook).

## Data Model
- **Event fields:** `message_id`, `date`, `from`, `reply_to`, `subject`, `dmarc`, `spf`, `dkim`, `urls[]`, `attachments[]`, `sha256[]`, `verdict`, `score`
- **IOC fields:** `type`, `value`, `first_seen`, `last_seen`, `sources[]`, `severity`, `tags[]`

## Testing
pytest -q

text
A sample corpus in `data/samples/` should contain benign and phishing examples; expected alerts can live under `tests/fixtures`.

## Security
- Do not commit API keys; use environment variables or a `.env` file.
- Validate and sanitize untrusted MIME and attachments to avoid parser exploits.

## Roadmap
- Add sandbox detonation metadata ingestion
- Integrate DMARC aggregate feedback parsing
- Export to STIX 2.1 and TAXII clients
- Optional stream‑processor mode with a message queue

## License
Choose a license (MIT, Apache‑2.0, etc.) and include a `LICENSE` file in the reposito
