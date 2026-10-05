# Jetski Harness

[![Platform: Jetski UI Sidecar](https://img.shields.io/badge/Platform-Jetski_UI_Sidecar-4285F4?style=flat-square&logo=googlechrome&logoColor=white)](#)
[![Backend: Python 3 Connect-RPC](https://img.shields.io/badge/Backend-Python_3_%2B_Connect--RPC-3776AB?style=flat-square&logo=python&logoColor=white)](#directory-structure)
[![UI: Customisable 2x2 Bento Grid](https://img.shields.io/badge/UI-Customisable_2x2_Bento_Grid-0F9D58?style=flat-square)](#key-features)
[![Telemetry: Live Token & Cache KPIs](https://img.shields.io/badge/Telemetry-Token_%26_Cache_KPIs-8E24AA?style=flat-square)](#key-features)

A unified custom UI plugin and mission control workspace for **Jetski / Antigravity** combining multi-session agent chat, an embedded execution DAG and architecture visualiser ([`Agent Tracer`](https://github.com/elim316/Jetski-Agent-Tracer-Plugin)), cron and daemon automation controls, and real-time token, context-cache, and cost telemetry.

---

## Key Features

1. **Customisable 2×2 Overview Bento Grid**
   - **Draggable Central Splitters:** Resize column widths and row heights on the fly (`--bento-col-split`, `--bento-row-split`), or double-click any splitter to snap back.
   - **Drag-and-Drop Card Swapping:** Grab the `⋮⋮` handle on any quadrant header to rearrange the 4 pillars in any order.
   - **One-Click Wide (`⇔`) Mode:** Expand any quadrant across both columns (`grid-column: 1 / -1`) when inspecting dense graphs or long chat streams.
   - **`↺ Reset Layout` Button:** One click in the top bar restores the default 2×2 proportions and card order.

2. **Multi-Session Chat & Live Subagent Mini-Feed**
   - Switch between any recent agent conversation (`◀`, dropdown, `▶`) or start a new session directly from the top bar.
   - **Cross-Pane Click-to-Sync:** Clicking any `tool` call chip or `Subagent` pill in Chat highlights and scrolls directly to that execution step inside the embedded **Agent Tracer**.
   - **Zero-Config Prompt Dispatch:** Automatically resolves the active model enum and workspace metadata from the local Language Server.

3. **Embedded Agent Tracer (`Timeline` & `Architecture` Modes)**
   - Works seamlessly in both full-bleed mode and compact Bento mode (`?compact=1`), with a dedicated `Timeline | Architecture` segmented toggle in the quadrant header.
   - Bi-directional **Light / Dark Theme Sync** (`postMessage` + `localStorage`).

4. **Interactive Token, Context Window & Automation Explainers**
   - Click any Token KPI card (**Session Tokens**, **Cache Hit Rate**, **Est. Session Cost**, **Recent Sessions**) or the **Context Window Saturation Gauge** (`~200k` compaction threshold) to view its exact formula and live session breakdown.
   - Click any **Scheduled Automation** or **Sidecar Service** row to inspect what the job does, its SGT vs. UTC schedule, its execution target, and its deduplication/safety guarantees.
   - **One-Click `Run Now` & `Pause / Resume`:** Trigger scheduled `agentapi` jobs on demand or toggle `restart_policy` (`"always"` ↔ `"never"`) directly from the UI.

---

## Directory Structure

```text
jetski-harness/
├── plugin.json                # Plugin manifest
└── sidecars/
    └── console/
        ├── sidecar.json       # SidecarManager AuxPane web UI manifest
        ├── server.py          # Python HTTP & JSON-RPC bridge to Language Server
        ├── index.html         # Resizable Bento Grid + 4 Pillar Views
        ├── styles.css         # Responsive Light/Dark theme stylesheet
        ├── app.js             # Frontend controller, grid resizer & explainers
        └── preload.js         # Sidecar token & postMessage bridge
```

## Installation

Place or clone this repository into `~/.gemini/config/plugins/jetski-harness`:

```bash
git clone https://github.com/elim316/Jetski-Harness.git ~/.gemini/config/plugins/jetski-harness
```

`SidecarManager` automatically discovers `sidecars/console/sidecar.json` and launches the **Jetski Harness** auxiliary pane.
