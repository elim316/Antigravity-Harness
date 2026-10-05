# Jetski Harness

[![Platform: Jetski UI Sidecar](https://img.shields.io/badge/Platform-Jetski_UI_Sidecar-4285F4?style=flat-square&logo=googlechrome&logoColor=white)](#)
[![Backend: Python 3 Connect-RPC](https://img.shields.io/badge/Backend-Python_3_%2B_Connect--RPC-3776AB?style=flat-square&logo=python&logoColor=white)](#directory-structure)
[![UI: Customisable 2x2 Bento Grid](https://img.shields.io/badge/UI-Customisable_2x2_Bento_Grid-0F9D58?style=flat-square)](#key-features)
[![Telemetry: Live Token and Cache KPIs](https://img.shields.io/badge/Telemetry-Token_%26_Cache_KPIs-8E24AA?style=flat-square)](#key-features)

A unified custom UI plugin and mission control workspace for Jetski and Antigravity. It combines multi-session agent chat, an embedded execution graph and architecture visualiser ([`Agent Tracer`](https://github.com/elim316/Jetski-Agent-Tracer-Plugin)), cron and daemon automation controls, and real-time token, context-cache, and cost telemetry.

---

## Key features

1. Customisable 2x2 overview Bento grid
   - Draggable central splitters let you resize column widths and row heights on the fly (`--bento-col-split`, `--bento-row-split`), or double-click any splitter to snap back.
   - Drag-and-drop card handles (`⋮⋮`) let you rearrange the four panels in any order.
   - One-click wide mode (`⇔`) expands any panel across both columns (`grid-column: 1 / -1`) when inspecting dense graphs or long chat streams.
   - A reset layout button (`↺ Reset Layout`) in the top bar restores the default 2x2 proportions and card order.

2. Multi-session chat and live subagent mini-feed
   - Switch between recent agent conversations (`◀`, dropdown, `▶`) or start a new session directly from the top bar.
   - Clicking any tool call chip or subagent pill in Chat highlights and scrolls directly to that execution step inside the embedded Agent Tracer.
   - Automatically resolves the active model enum and workspace metadata from the local Language Server.

3. Embedded Agent Tracer (Timeline and Architecture modes)
   - Runs in both full-bleed mode and compact Bento mode (`?compact=1`), with a dedicated `Timeline | Architecture` toggle in the panel header.
   - Synchronises light and dark themes across frames via `postMessage` and `localStorage`.

4. Interactive token, context window, and automation explainers
   - Click any token KPI card (Session Tokens, Cache Hit Rate, Est. Session Cost, Recent Sessions) or the Context Window Saturation gauge (`~200k` compaction threshold) to view its exact formula and live session breakdown.
   - Click any scheduled automation or sidecar service row to inspect what the job does, its SGT versus UTC schedule, its execution target, and its deduplication logic.
   - Trigger scheduled `agentapi` jobs on demand (`Run Now`) or toggle `restart_policy` (`"always"` / `"never"`) directly from the UI.

---

## Directory structure

```text
jetski-harness/
├── plugin.json                # Plugin manifest
└── sidecars/
    └── console/
        ├── sidecar.json       # SidecarManager AuxPane web UI manifest
        ├── server.py          # Python HTTP and JSON-RPC bridge to Language Server
        ├── index.html         # Customisable Bento Grid and 4 Pillar Views
        ├── styles.css         # Responsive Light/Dark theme stylesheet
        ├── app.js             # Frontend controller, grid resizer, and explainers
        └── preload.js         # Sidecar token and postMessage bridge
```

## Installation

Clone this repository into `~/.gemini/config/plugins/jetski-harness`:

```bash
git clone https://github.com/elim316/Jetski-Harness.git ~/.gemini/config/plugins/jetski-harness
```

`SidecarManager` automatically discovers `sidecars/console/sidecar.json` and launches the Jetski Harness auxiliary pane.
