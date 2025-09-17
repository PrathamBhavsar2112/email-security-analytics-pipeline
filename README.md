Email Security Analytics Pipeline
A modular Python pipeline for ingesting raw email artifacts, extracting indicators, enriching with threat intelligence, classifying phishing/malware campaigns, and producing analyst-ready reports and alerts.

Features
MIME/EML ingestion with header/body/attachment parsing for large batches of messages.

Indicator extraction: URLs, domains, IPs, hashes, senders, DKIM/SPF/DMARC signals.

Threat intel enrichment via pluggable providers (HTTP API adapters, local reputation lists).

Rule and ML-based detection for phishing, BEC, malware-linked URLs, and spoofing patterns.

Alerting and reporting to JSON/CSV, with optional webhook/email notifications.

Config-driven pipeline stages to enable/disable modules without code changes.

Architecture
Ingestion: reads .eml/.msg or raw RFC822 from a folder or stdin and normalizes to structured records.

Parsing & Extraction: splits headers/body/attachments; extracts indicators and email-auth results.

Enrichment: queries TI sources; caches results for repeated IOCs.

Detection: applies signature rules plus an optional classifier; assigns severity and categories.

Output: writes events, IOCs, and alerts to out/ as JSON/CSV; can forward to a webhook.

Getting Started
Prerequisites:

Python 3.10+ and pip.

Optional: virtualenv or venv for isolated installs.

Setup:

Clone the repository and create a virtual environment, then install dependencies (add to requirements.txt as needed).

Example:

Place sample .eml files under data/input/ and run the pipeline to produce outputs in out/.

Configuration
config.yaml controls input paths, enabled modules, enrichment providers, thresholds, and output locations.

Secrets (API keys) should be provided via environment variables or a .env file ignored by VCS.

Sample settings:

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

min_score: 0.6.

Usage
Process a directory:

python -m pipeline.run --config config.yaml --input data/input --output out.

Single file:

python -m pipeline.run --file data/input/sample.eml.

Enable debug logs:

python -m pipeline.run --config config.yaml --log-level DEBUG.

Outputs:

out/events.jsonl — normalized per-message events.

out/iocs.csv — aggregated indicators (url, domain, ip, hash, first_seen, sources).

out/alerts.json — detections with severity, rule/model hits, and evidence excerpts.

Detection Logic
Rules: YAML rules that match header anomalies, suspicious TLDs, look‑alike senders, and URL patterns.

ML (optional): simple text/URL features (brand terms, obfuscation, shortening, IP URLs) for a binary phishing score.

Email auth: DKIM/SPF/DMARC parsing to flag spoofing and alignment failures.

Extending
Add a new enrichment provider by implementing EnricherBase and registering it in config.yaml.

Add detection rules in rules/*.yml; each rule defines conditions, score, and tags.

Implement new output sinks by subclassing WriterBase (e.g., send to SIEM webhook).

Data Model
Core event fields (example):

message_id, date, from, reply_to, subject, dmarc, spf, dkim, urls[], attachments[], sha256[], verdict, score.

IOC fields:

type, value, first_seen, last_seen, sources[], severity, tags[].

Testing
Unit tests: run pytest -q.

Sample corpus: data/samples/ contains benign and phishing examples; expected alerts under tests/fixtures.

Operational Guidance
Batch size and backoff for TI APIs are configurable; enable local caching to minimize rate limits.

Treat network enrichment as best-effort; detections should still function offline using rules.

Sanitize and redact PII when exporting sharable reports.

Roadmap
Add sandbox detonation metadata ingestion (e.g., URL/attachment behavior).

Integrate DMARC aggregate feedback parsing.

Export to STIX 2.1 and TAXII clients.

Optional stream processor mode with a message queue.

Repository Structure
pipeline/

ingest.py, parse.py, extract.py, enrich/, detect/, output/.

rules/

config.yaml

data/input/, out/

tests/.

Security
Do not commit API keys; prefer environment variables and .env in .gitignore.

Validate and sanitize untrusted MIME and attachments before processing to avoid parser exploits.

License
Choose a license (e.g., MIT or Apache-2.0) and include LICENSE in the repository root.

Contributing
Open an issue with proposed changes; submit PRs with tests and documentation updates.

Acknowledgments
Inspired by common threat-intel and email-security pipelines and standard README guidance for clarity and setup flow.
