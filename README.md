# Ops Automation Toolkit

> Practical tools and reference designs from IT service delivery and operations work: the small automations that save real hours.

| Tool | Folder | What it does |
|---|---|---|
| **Bookmarks Organizer** | `bookmarks-organizer/` | Python CLI that parses any browser's bookmark export, checks every link in parallel (32 workers), auto-categorizes by domain and content type, and writes a clean re-importable file plus a CSV health report. |
| **ServiceNow Ops Dashboard** | `servicenow-ops-dashboard/` | Single-file HTML dashboard mock for incident and change KPIs (sample data only), for showing leadership how to visualize ITSM health. |
| **SOP Builder** | `sop-builder/` | Browser app that turns steps, roles and warnings into a consistently formatted Standard Operating Procedure. |
| **Hybrid Agent Orchestrator Design** | `architecture-docs/` | Reference architecture for combining autonomous event-driven agents (inventory, migration, reporting) with centralized governance orchestrators (compliance, physical ops) over a message bus. |
| **Check-Port** | `network-tools/` | PowerShell TCP port reachability check for quick triage. |

## Usage

```bash
python bookmarks-organizer/organize_bookmarks.py favorites.html organized.html --workers 64
```
```powershell
./network-tools/Check-Port.ps1 -targetHost example.com -port 443
```
The HTML tools open directly in a browser; there's no build step.

## Roadmap

- Wire the ServiceNow dashboard to the Table API (read-only, env-var credentials)
- Package the bookmarks organizer on PyPI with tests

## Topics
`automation` `itsm` `servicenow` `powershell` `python` `sop` `operations` `ai-agents` `architecture`
