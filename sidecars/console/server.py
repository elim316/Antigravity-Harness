#!/usr/bin/env python3
"""Custom Jetski Harness & Mission Control Sidecar Backend (v2.9).

Provides:
1. Real-time Token Usage & Cost Telemetry (via Language Server Connect-RPC
   GetCascadeTrajectory + transcript fallback, with model-tier-aware pricing).
2. Live Running Agents & Conversations monitor (merges GetAllCascadeTrajectories
   with byte-offset incremental tailing of transcript.jsonl).
3. Automations & Sidecars monitor (scans ~/.gemini/config/sidecars, plugins,
   and builtin sidecars; matches live OS PIDs & uptimes; translates UTC crons
   to SGT UTC+8; reads local JSON state trackers and sidecar logs).
4. Jetski Runtime & Workspace MCP Status (Google Workspace MCP servers,
   tool counts, OAuth token status, Memory FUSE health, Language Server status).
5. Full compatibility & dynamic conversation switching for the embedded Agent
   Tracer view (/tracer, /api/transcript, /api/step_full, /api/subagents_status,
   /api/telemetry, /api/conversations, /api/update-status, /api/update).
6. Agent dispatch endpoints (/api/chat/send, /api/chat/stop, /api/automation/toggle,
   /api/automation/trigger, /_sidecar/send-message, /_sidecar/new-conversation).
"""

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.expanduser("~")
JETSKI_DIR = os.path.join(HOME_DIR, ".gemini", "jetski")
BRAIN_DIR = os.path.join(JETSKI_DIR, "brain")
CONFIG_DIR = os.path.join(HOME_DIR, ".gemini", "config")
MEMORY_DIR = os.path.join(HOME_DIR, "memory", "default")
TRACER_DIR = os.path.join(
    CONFIG_DIR, "plugins", "agent-tracer", "sidecars", "tracer"
)
PRELOAD_SDK_PATH = os.path.join(BASE_DIR, "preload.js")

SGT_TZ = timezone(timedelta(hours=8), name="SGT")
_UUID_RE = re.compile(r"^[a-fA-F0-9-]+$")
_CONV_DIR_RE = re.compile(r"^[a-fA-F0-9-]{20,}$")

# Bypass corporate/Cloudtop HTTP proxy for all 127.0.0.1 Language Server RPC calls
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# Single auto-reload watcher: exit cleanly when server.py is modified so
# SidecarManager (restart_policy: "always") respawns with updated code.
_SELF_FILE = os.path.abspath(__file__)
try:
  _STARTUP_MTIME = os.path.getmtime(_SELF_FILE)
except OSError:
  _STARTUP_MTIME = None


def _watch_self():
  while True:
    time.sleep(5.0)
    try:
      if _STARTUP_MTIME and os.path.getmtime(_SELF_FILE) != _STARTUP_MTIME:
        print(
            "[jetski-harness] server.py modified, exiting for auto-restart...",
            flush=True,
        )
        os._exit(0)
    except OSError:
      pass


threading.Thread(target=_watch_self, daemon=True).start()

# Thread-safe bounded LRU caches
_CACHE_LOCK = threading.Lock()
_MAX_CONV_CACHE = 64

_LS_CONN_CACHE = {"address": None, "csrf": None, "pid": None, "checked_at": 0.0}
_STEP_CACHE: OrderedDict[str, dict] = OrderedDict()
_FULL_STEP_CONTENT_CACHE: OrderedDict[tuple[str, int], dict] = OrderedDict()
_TOKEN_USAGE_CACHE: OrderedDict[str, tuple[int, float, dict]] = OrderedDict()
_CHAT_STREAM_CACHE: OrderedDict[str, tuple[int, int, dict]] = OrderedDict()
_STATIC_FILE_CACHE: dict[str, tuple[int, int, bytes]] = {}
_UPDATE_CACHE = {"data": None, "ts": 0.0}
_UPDATE_TTL = 300.0

_LS_TRAJECTORIES_CACHE = {"ts": 0.0, "summaries": {}}
_BRAIN_ENTRIES_CACHE = {"ts": 0.0, "ids": set()}
_AUTOMATIONS_CACHE = {"ts": 0.0, "data": []}
_RUNTIME_STATUS_CACHE = {"ts": 0.0, "data": {}}
_MODEL_LABEL_CACHE = {"ts": 0.0, "map": {}}


def _lru_get(cache: OrderedDict, key: str):
  with _CACHE_LOCK:
    if key in cache:
      cache.move_to_end(key)
      return cache[key]
    return None


def _lru_set(cache: OrderedDict, key: str, value, max_size: int = _MAX_CONV_CACHE):
  with _CACHE_LOCK:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_size:
      cache.popitem(last=False)


def _read_cached_file_bytes(fpath: str) -> bytes | None:
  """Reads static file bytes with in-memory mtime_ns caching."""
  try:
    st = os.stat(fpath)
  except OSError:
    return None
  with _CACHE_LOCK:
    cached = _STATIC_FILE_CACHE.get(fpath)
    if cached and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
      return cached[2]
  try:
    with open(fpath, "rb") as f:
      data = f.read()
  except OSError:
    return None
  with _CACHE_LOCK:
    _STATIC_FILE_CACHE[fpath] = (st.st_mtime_ns, st.st_size, data)
  return data


def _discover_language_server(force: bool = False):
  """Finds the active Jetski Language Server HTTP address and CSRF token."""
  now = time.time()
  if (
      not force
      and _LS_CONN_CACHE["address"]
      and _LS_CONN_CACHE["csrf"]
      and (now - _LS_CONN_CACHE["checked_at"] < 30.0)
  ):
    return (
        _LS_CONN_CACHE["address"],
        _LS_CONN_CACHE["csrf"],
        _LS_CONN_CACHE["pid"],
    )

  env_addr = os.environ.get("ANTIGRAVITY_LS_ADDRESS")
  env_csrf = os.environ.get("ANTIGRAVITY_CSRF_TOKEN")
  if env_addr and env_csrf and not force:
    _LS_CONN_CACHE.update(
        {"address": env_addr, "csrf": env_csrf, "pid": None, "checked_at": now}
    )
    return env_addr, env_csrf, None

  try:
    res = subprocess.run(
        ["ps", "-eo", "pid,args"],
        capture_output=True,
        text=True,
        timeout=3,
    )
    for line in res.stdout.splitlines():
      if "language_server" in line and "--csrf_token" in line:
        parts = line.strip().split(None, 1)
        if len(parts) < 2:
          continue
        pid, cmd = parts[0], parts[1]
        csrf_m = re.search(r"--csrf_token[=\s]+([a-fA-F0-9-]+)", cmd)
        if not csrf_m:
          continue
        csrf = csrf_m.group(1)
        ss_res = subprocess.run(
            ["ss", "-tlpn"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        ports = []
        for sline in ss_res.stdout.splitlines():
          if f"pid={pid}," in sline:
            pm = re.search(r"127\.0\.0\.1:(\d+)", sline)
            if pm:
              ports.append(int(pm.group(1)))
        for pt in sorted(ports):
          addr = f"127.0.0.1:{pt}"
          try:
            req = urllib.request.Request(
                f"http://{addr}/exa.language_server_pb.LanguageServerService/GetMcpServerStates",
                data=b"{}",
                headers={
                    "Content-Type": "application/json",
                    "x-codeium-csrf-token": csrf,
                },
                method="POST",
            )
            with _NO_PROXY_OPENER.open(req, timeout=0.8) as resp:
              if resp.status == 200:
                _LS_CONN_CACHE.update({
                    "address": addr,
                    "csrf": csrf,
                    "pid": int(pid),
                    "checked_at": now,
                })
                return addr, csrf, int(pid)
          except Exception:
            continue
  except Exception:
    pass

  return env_addr, env_csrf, None


def _call_ls(method: str, payload: dict | None = None, timeout: float = 1.5):
  """Calls a Connect-RPC method on the local Jetski Language Server."""
  addr, csrf, _ = _discover_language_server(force=False)
  if not addr or not csrf:
    return None
  body = json.dumps(payload or {}).encode("utf-8")
  for attempt in range(2):
    try:
      req = urllib.request.Request(
          f"http://{addr}/exa.language_server_pb.LanguageServerService/{method}",
          data=body,
          headers={
              "Content-Type": "application/json",
              "x-codeium-csrf-token": csrf,
          },
          method="POST",
      )
      with _NO_PROXY_OPENER.open(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError:
      return None
    except Exception:
      if attempt == 0:
        addr, csrf, _ = _discover_language_server(force=True)
        if not addr or not csrf:
          break
  return None


def _parse_duration_seconds(val) -> float:
  if not val:
    return 0.0
  if isinstance(val, (int, float)):
    return float(val)
  if isinstance(val, str) and val.endswith("s"):
    try:
      return float(val[:-1])
    except ValueError:
      return 0.0
  return 0.0


# Model-tier-aware pricing rates (USD per 1M tokens)
_MODEL_PRICING_TIERS = {
    "flash_lite": {"inputPer1M": 0.10, "cachedPer1M": 0.025, "outputPer1M": 0.40},
    "flash": {"inputPer1M": 0.30, "cachedPer1M": 0.075, "outputPer1M": 2.50},
    "claude_opus": {"inputPer1M": 15.00, "cachedPer1M": 1.50, "outputPer1M": 75.00},
    "claude_sonnet": {"inputPer1M": 3.00, "cachedPer1M": 0.30, "outputPer1M": 15.00},
    "pro": {"inputPer1M": 1.25, "cachedPer1M": 0.3125, "outputPer1M": 10.00},
}


def _get_pricing_rates(model_label: str = "", raw_model: str = "") -> dict:
  """Selects pricing rates based on resolved model label or enum."""
  combined = f"{model_label} {raw_model}".lower()
  if "flash lite" in combined or "flash_lite" in combined or "m198" in combined:
    return _MODEL_PRICING_TIERS["flash_lite"]
  if "flash" in combined or any(k in combined for k in ("m196", "m200", "m264", "m265", "m298")):
    return _MODEL_PRICING_TIERS["flash"]
  if "opus" in combined or "m65" in combined:
    return _MODEL_PRICING_TIERS["claude_opus"]
  if "sonnet" in combined or "claude" in combined or "m64" in combined:
    return _MODEL_PRICING_TIERS["claude_sonnet"]
  return _MODEL_PRICING_TIERS["pro"]


def _estimate_cost_usd(
    uncached_in: int,
    output_tok: int,
    thinking_tok: int,
    cache_tok: int,
    model_label: str = "",
    raw_model: str = "",
) -> float:
  """Estimates USD cost using model-tier-aware pricing rates."""
  rates = _get_pricing_rates(model_label, raw_model)
  cost = (
      (max(0, uncached_in) / 1_000_000.0) * rates["inputPer1M"]
      + (max(0, cache_tok) / 1_000_000.0) * rates["cachedPer1M"]
      + (max(0, output_tok + thinking_tok) / 1_000_000.0) * rates["outputPer1M"]
  )
  return round(cost, 4)


def _get_transcript_path(conv_id: str, full: bool = False) -> str | None:
  if not conv_id or not _UUID_RE.match(conv_id):
    return None
  fname = "transcript_full.jsonl" if full else "transcript.jsonl"
  for app_name in ("jetski", "antigravity"):
    candidate = os.path.join(
        HOME_DIR,
        ".gemini",
        app_name,
        "brain",
        conv_id,
        ".system_generated",
        "logs",
        fname,
    )
    if os.path.isfile(candidate):
      return candidate
  return os.path.join(BRAIN_DIR, conv_id, ".system_generated", "logs", fname)


def _get_cached_transcript_bundle(conv_id: str) -> dict:
  """Incrementally tails transcript.jsonl by byte offset and maintains both parsed steps and summary metadata."""
  tpath = _get_transcript_path(conv_id, full=False)
  if not tpath or not os.path.isfile(tpath):
    return {
        "exists": False,
        "mtime": 0.0,
        "mtime_ns": 0,
        "size": 0,
        "steps": [],
        "summary": {
            "exists": False,
            "stepCount": 0,
            "toolCallCount": 0,
            "subagentCount": 0,
            "title": "",
            "status": "IDLE",
            "turnCompleted": False,
            "lastStepType": "",
            "lastAction": "",
            "updatedAt": "",
            "createdAt": "",
            "charCount": 0,
            "mtime": 0.0,
        },
    }

  try:
    st = os.stat(tpath)
    mtime = st.st_mtime
    mtime_ns = st.st_mtime_ns
    fsize = st.st_size
  except OSError:
    mtime, mtime_ns, fsize = 0.0, 0, 0

  cached = _lru_get(_STEP_CACHE, conv_id)
  if cached and cached["mtime_ns"] == mtime_ns and cached["size"] == fsize:
    # Re-evaluate time-dependent status if the file was modified recently
    summary = cached["summary"]
    if mtime and (time.time() - mtime < 30.0):
      summary = dict(summary)
      summary["status"] = _compute_step_status(
          summary.get("lastStepType", ""),
          summary.get("lastStepStatus", ""),
          summary.get("turnCompleted", False),
          mtime,
      )
    return {**cached, "summary": summary}

  # Determine whether we can incrementally read from the previous byte offset
  if cached and fsize >= cached["offset"] and cached["offset"] > 0:
    offset = cached["offset"]
    steps = list(cached["steps"])
    meta_state = dict(cached["meta_state"])
  else:
    offset = 0
    steps = []
    meta_state = {
        "step_count": 0,
        "tool_count": 0,
        "subagent_count": 0,
        "first_user_text": "",
        "created_at": "",
        "updated_at": "",
        "last_step_type": "",
        "last_step_status": "",
        "last_step_has_tools": False,
        "last_step_has_content": False,
        "last_action": "",
        "char_count": 0,
    }

  try:
    with open(tpath, "rb") as f:
      if offset > 0:
        f.seek(offset)
      raw_chunk = f.read()
      new_offset = f.tell()
  except OSError:
    raw_chunk = b""
    new_offset = offset

  if raw_chunk:
    # Only process complete newline-terminated lines so partial writes are retried next poll
    last_nl = raw_chunk.rfind(b"\n")
    if last_nl == -1:
      processable = b""
      new_offset = offset
    else:
      processable = raw_chunk[: last_nl + 1]
      new_offset = offset + last_nl + 1

    for raw_line in processable.decode("utf-8", errors="replace").splitlines():
      line = raw_line.strip()
      if not line:
        continue
      meta_state["char_count"] += len(line)
      try:
        obj = json.loads(line)
      except json.JSONDecodeError:
        continue

      steps.append(obj)
      meta_state["step_count"] += 1
      stype = obj.get("type", "")
      sstatus = obj.get("status", "")
      screated = obj.get("created_at", "")
      if not meta_state["created_at"] and screated:
        meta_state["created_at"] = screated
      if screated:
        meta_state["updated_at"] = screated
      meta_state["last_step_type"] = stype
      meta_state["last_step_status"] = sstatus

      if stype == "USER_INPUT" and not meta_state["first_user_text"]:
        content = (obj.get("content") or "").strip()
        content = re.sub(r"</?USER_REQUEST>", "", content).strip()
        if content:
          meta_state["first_user_text"] = content.splitlines()[0][:90]

      tcalls = obj.get("tool_calls") or []
      if stype == "PLANNER_RESPONSE":
        meta_state["last_step_has_tools"] = bool(isinstance(tcalls, list) and tcalls)
        meta_state["last_step_has_content"] = bool((obj.get("content") or "").strip())

      if isinstance(tcalls, list) and tcalls:
        meta_state["tool_count"] += len(tcalls)
        for tc in tcalls:
          tname = tc.get("name", "")
          if tname == "invoke_subagent":
            meta_state["subagent_count"] += 1
          args = tc.get("arguments") or tc.get("args") or {}
          if isinstance(args, dict):
            summary_str = args.get("toolAction") or args.get("toolSummary") or tname
            if isinstance(summary_str, str):
              summary_str = summary_str.strip().strip('"').strip()
            if summary_str:
              meta_state["last_action"] = summary_str

  updated_at = meta_state["updated_at"]
  if not updated_at and mtime:
    updated_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

  turn_completed = (
      meta_state["last_step_type"] == "PLANNER_RESPONSE"
      and meta_state["last_step_status"] == "DONE"
      and meta_state["last_step_has_content"]
      and not meta_state["last_step_has_tools"]
  )
  status = _compute_step_status(
      meta_state["last_step_type"],
      meta_state["last_step_status"],
      turn_completed,
      mtime,
  )

  summary = {
      "exists": True,
      "stepCount": meta_state["step_count"],
      "toolCallCount": meta_state["tool_count"],
      "subagentCount": meta_state["subagent_count"],
      "title": meta_state["first_user_text"],
      "status": status,
      "turnCompleted": turn_completed,
      "lastStepType": meta_state["last_step_type"],
      "lastStepStatus": meta_state["last_step_status"],
      "lastAction": meta_state["last_action"],
      "updatedAt": updated_at,
      "createdAt": meta_state["created_at"],
      "charCount": meta_state["char_count"],
      "mtime": mtime,
  }

  bundle = {
      "exists": True,
      "mtime": mtime,
      "mtime_ns": mtime_ns,
      "size": fsize,
      "offset": new_offset,
      "steps": steps,
      "meta_state": meta_state,
      "summary": summary,
  }
  _lru_set(_STEP_CACHE, conv_id, bundle)
  return bundle


def _compute_step_status(
    last_step_type: str, last_step_status: str, turn_completed: bool, mtime: float
) -> str:
  age_sec = time.time() - mtime if mtime else 999999.0
  if last_step_status in ("RUNNING", "IN_PROGRESS", "PENDING"):
    return "RUNNING"
  if not turn_completed and age_sec < 15.0 and last_step_type in (
      "USER_INPUT",
      "PLANNER_RESPONSE",
      "GENERIC",
  ):
    return "RUNNING"
  if last_step_status == "ERROR":
    return "ERROR"
  return "IDLE"


def _summarize_transcript_fast(conv_id: str) -> dict:
  """Returns transcript summary metadata from the shared incremental step cache."""
  return _get_cached_transcript_bundle(conv_id)["summary"]


_KNOWN_PLACEHOLDER_LABELS = {
    "MODEL_PLACEHOLDER_M260": "Gemini Next",
    "MODEL_PLACEHOLDER_M37": "Gemini Pro",
    "MODEL_PLACEHOLDER_M64": "Claude Sonnet 4.6 (Thinking)",
    "MODEL_PLACEHOLDER_M65": "Claude Opus 4.6 (Thinking)",
    "MODEL_PLACEHOLDER_M256": "Gemini 3.5 Pro",
    "MODEL_PLACEHOLDER_M257": "Gemini 3.5 Pro (Low Thinking)",
    "MODEL_PLACEHOLDER_M273": "Gemini 3.5 Pro",
    "MODEL_PLACEHOLDER_M196": "Gemini 3.6 Flash",
    "MODEL_PLACEHOLDER_M198": "Gemini 3.5 Flash Lite",
    "MODEL_PLACEHOLDER_M200": "Gemini 3.5 Flash",
    "MODEL_PLACEHOLDER_M264": "Gemini 3.6 Flash (High)",
    "MODEL_PLACEHOLDER_M265": "Gemini 3.6 Flash",
    "MODEL_PLACEHOLDER_M298": "Gemini 3.7 Flash",
    "MODEL_GOOGLE_GEMINI_2_5_PRO": "Gemini 2.5 Pro",
    "MODEL_GOOGLE_GEMINI_2_5_FLASH": "Gemini 2.5 Flash",
    "MODEL_GOOGLE_GEMINI_INTERNAL_BYOM": "Gemini Internal",
}


def _get_model_display_name(raw_model: str) -> str:
  """Translates internal MODEL_PLACEHOLDER_* proto enums into friendly model names."""
  if not raw_model:
    return "Gemini Next"
  now = time.time()
  if not _MODEL_LABEL_CACHE["map"] or (now - _MODEL_LABEL_CACHE["ts"] > 300.0):
    label_map = dict(_KNOWN_PLACEHOLDER_LABELS)
    try:
      avail = _call_ls("GetAvailableModels", {}, timeout=1.5)
      if isinstance(avail, dict):
        models = (avail.get("response") or {}).get("models") or {}
        for info in models.values():
          if isinstance(info, dict) and info.get("model") and info.get("displayName"):
            label_map[info["model"]] = info["displayName"]
      cfg_data = _call_ls("GetCascadeModelConfigData", {}, timeout=1.5)
      if isinstance(cfg_data, dict):
        for cfg in cfg_data.get("clientModelConfigs") or []:
          if isinstance(cfg, dict):
            m_enum = (cfg.get("modelOrAlias") or {}).get("model")
            lbl = cfg.get("label")
            if m_enum and lbl:
              label_map[m_enum] = lbl
    except Exception:
      pass
    _MODEL_LABEL_CACHE["map"] = label_map
    _MODEL_LABEL_CACHE["ts"] = now

  mapped = _MODEL_LABEL_CACHE["map"].get(raw_model)
  if mapped:
    return mapped
  if raw_model.startswith("MODEL_PLACEHOLDER_"):
    return "Gemini Next"
  return (
      raw_model.replace("MODEL_GOOGLE_", "")
      .replace("MODEL_", "")
      .replace("_", " ")
      .title()
  )


def _get_token_telemetry(
    conv_id: str, include_generations: bool = True, allow_rpc: bool = True
) -> dict:
  """Fetches exact token telemetry for conv_id via GetCascadeTrajectory."""
  bundle = _get_cached_transcript_bundle(conv_id)
  tmeta = bundle["summary"]
  mtime_ns = bundle["mtime_ns"]
  mtime = tmeta.get("mtime", 0.0)
  now = time.time()

  cached = _lru_get(_TOKEN_USAGE_CACHE, conv_id)
  ttl = 12.0 if (now - mtime < 30.0) else 300.0
  if cached and cached[0] == mtime_ns and (now - cached[1] < ttl):
    return cached[2]
  if not allow_rpc and cached:
    return cached[2]

  traj_resp = (
      _call_ls("GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.0)
      if allow_rpc
      else None
  )
  gen_meta = []
  ls_status = ""
  if traj_resp and isinstance(traj_resp.get("trajectory"), dict):
    traj = traj_resp["trajectory"]
    gen_meta = traj.get("generatorMetadata") or []
    ls_status = traj.get("status", "")

  if gen_meta:
    uncached_in_tok = 0
    out_tok = 0
    think_tok = 0
    cache_tok = 0
    ttft_sum = 0.0
    ttft_count = 0
    stream_sum = 0.0
    model_name = ""
    api_provider = ""
    generations = []

    for idx, gm in enumerate(gen_meta):
      cm = gm.get("chatModel") or {}
      usage = cm.get("usage") or {}
      m_name = cm.get("model") or usage.get("model") or ""
      if m_name:
        model_name = m_name
      prov = usage.get("apiProvider") or ""
      if prov:
        api_provider = prov.replace("API_PROVIDER_", "")

      i_t = int(usage.get("inputTokens") or 0)
      o_t = int(usage.get("outputTokens") or 0)
      th_t = int(usage.get("thinkingOutputTokens") or 0)
      c_t = int(usage.get("cacheReadTokens") or 0)
      prompt_t = i_t + c_t
      ttft = _parse_duration_seconds(cm.get("timeToFirstToken"))
      sdur = _parse_duration_seconds(cm.get("streamingDuration"))
      step_indices = gm.get("stepIndices") or []

      uncached_in_tok += i_t
      out_tok += o_t
      think_tok += th_t
      cache_tok += c_t
      if ttft > 0:
        ttft_sum += ttft
        ttft_count += 1
      stream_sum += sdur

      generations.append({
          "turn": idx + 1,
          "stepIndex": step_indices[0] if step_indices else idx,
          "inputTokens": prompt_t,
          "promptTokens": prompt_t,
          "uncachedInputTokens": i_t,
          "outputTokens": o_t,
          "thinkingTokens": th_t,
          "cacheReadTokens": c_t,
          "cachedTokens": c_t,
          "ttftSeconds": round(ttft, 2),
          "streamingSeconds": round(sdur, 2),
          "model": _get_model_display_name(m_name),
          "rawModel": m_name,
      })

    total_prompt_tok = uncached_in_tok + cache_tok
    total_tok = total_prompt_tok + out_tok + think_tok
    cache_hit_pct = (
        round((cache_tok / total_prompt_tok) * 100.0, 1)
        if total_prompt_tok > 0
        else 0.0
    )
    last_gen = generations[-1] if generations else {}
    context_window_tokens = last_gen.get("inputTokens", 0) + last_gen.get(
        "outputTokens", 0
    )
    raw_m = model_name or "MODEL_PLACEHOLDER_M260"
    resolved_label = _get_model_display_name(raw_m)
    recent_gens = generations[-24:]
    pricing_rates = _get_pricing_rates(resolved_label, raw_m)

    result = {
        "conversationId": conv_id,
        "isEstimated": False,
        "lsStatus": ls_status,
        "model": resolved_label,
        "rawModel": raw_m,
        "pricingRates": pricing_rates,
        "apiProvider": api_provider or "INTERNAL",
        "llmCalls": len(generations),
        "inputTokens": total_prompt_tok,
        "promptTokens": total_prompt_tok,
        "uncachedInputTokens": uncached_in_tok,
        "outputTokens": out_tok,
        "thinkingTokens": think_tok,
        "cacheReadTokens": cache_tok,
        "cachedTokens": cache_tok,
        "totalTokens": total_tok,
        "cacheHitRatePct": cache_hit_pct,
        "contextWindowTokens": context_window_tokens,
        "estimatedCostUsd": _estimate_cost_usd(
            uncached_in_tok, out_tok, think_tok, cache_tok, resolved_label, raw_m
        ),
        "avgTtftSeconds": round(ttft_sum / ttft_count, 2) if ttft_count else 0.0,
        "totalStreamingSeconds": round(stream_sum, 1),
        "lastTurn": last_gen,
        "generations": recent_gens if include_generations else [],
        "perTurn": recent_gens if include_generations else [],
    }
    _lru_set(_TOKEN_USAGE_CACHE, conv_id, (mtime_ns, now, result))
    return result

  # Fallback estimation from transcript character count if trajectory isn't in LS memory
  est_out = max(1, tmeta.get("charCount", 0) // 4)
  est_in = est_out * max(1, min(tmeta.get("stepCount", 1), 12))
  est_cache = int(est_in * 0.78)
  est_uncached = max(0, est_in - est_cache)
  est_think = int(est_out * 0.35)
  default_label = _get_model_display_name("MODEL_PLACEHOLDER_M260")
  result = {
      "conversationId": conv_id,
      "isEstimated": True,
      "lsStatus": ls_status or "ARCHIVED",
      "model": default_label,
      "rawModel": "MODEL_PLACEHOLDER_M260",
      "pricingRates": _get_pricing_rates(default_label, "MODEL_PLACEHOLDER_M260"),
      "apiProvider": "GOOGLE_GEMINI_INTERNAL",
      "llmCalls": max(1, tmeta.get("stepCount", 1) // 2),
      "inputTokens": est_in,
      "promptTokens": est_in,
      "uncachedInputTokens": est_uncached,
      "outputTokens": est_out,
      "thinkingTokens": est_think,
      "cacheReadTokens": est_cache,
      "cachedTokens": est_cache,
      "totalTokens": est_in + est_out,
      "cacheHitRatePct": 78.0 if est_in > 0 else 0.0,
      "contextWindowTokens": min(est_in, 128000),
      "estimatedCostUsd": _estimate_cost_usd(
          est_uncached, est_out, est_think, est_cache, default_label
      ),
      "avgTtftSeconds": 0.0,
      "totalStreamingSeconds": 0.0,
      "lastTurn": {},
      "generations": [],
      "perTurn": [],
  }
  _lru_set(_TOKEN_USAGE_CACHE, conv_id, (mtime_ns, now, result))
  return result


def _list_conversations(active_conv_id: str) -> tuple[list[dict], dict]:
  """Lists recent/active conversations and computes global token summary."""
  now = time.time()
  if _LS_TRAJECTORIES_CACHE["summaries"] and (
      now - _LS_TRAJECTORIES_CACHE["ts"] < 6.0
  ):
    summaries = _LS_TRAJECTORIES_CACHE["summaries"]
  else:
    ls_all = _call_ls("GetAllCascadeTrajectories", {}, timeout=1.8) or {}
    summaries = ls_all.get("trajectorySummaries") or {}
    _LS_TRAJECTORIES_CACHE.update({"ts": now, "summaries": summaries})

  conv_ids = set(summaries.keys())
  if _BRAIN_ENTRIES_CACHE["ids"] and (now - _BRAIN_ENTRIES_CACHE["ts"] < 12.0):
    conv_ids.update(_BRAIN_ENTRIES_CACHE["ids"])
  elif os.path.isdir(BRAIN_DIR):
    brain_ids = set()
    try:
      for entry in os.listdir(BRAIN_DIR):
        if _CONV_DIR_RE.match(entry):
          tpath = _get_transcript_path(entry, full=False)
          if tpath and os.path.isfile(tpath):
            brain_ids.add(entry)
    except OSError:
      pass
    _BRAIN_ENTRIES_CACHE.update({"ts": now, "ids": brain_ids})
    conv_ids.update(brain_ids)

  items = []
  for cid in conv_ids:
    ls_info = summaries.get(cid) or {}
    tmeta = _summarize_transcript_fast(cid)
    ls_status = (ls_info.get("status") or "").replace("CASCADE_RUN_STATUS_", "")
    if ls_status in ("RUNNING", "IN_PROGRESS", "ACTIVE"):
      status = "RUNNING"
    elif ls_status in ("IDLE", "COMPLETED", "DONE"):
      status = "IDLE" if tmeta["status"] != "RUNNING" else "RUNNING"
    else:
      status = tmeta["status"]

    if (
        cid == active_conv_id
        and status == "IDLE"
        and not tmeta.get("turnCompleted")
    ):
      if time.time() - tmeta.get("mtime", 0) < 12:
        status = "RUNNING"

    title = (
        ls_info.get("summary")
        or tmeta.get("title")
        or f"Session {cid[:8]}"
    )
    updated_at = (
        ls_info.get("lastModifiedTime")
        or tmeta.get("updatedAt")
        or ""
    )
    created_at = (
        ls_info.get("createdTime")
        or tmeta.get("createdAt")
        or ""
    )
    step_count = max(
        int(ls_info.get("stepCount") or 0), tmeta.get("stepCount", 0)
    )

    workspaces = ls_info.get("workspaces") or []
    ws_label = "No Workspace (Scratch)"
    if workspaces and isinstance(workspaces[0], dict):
      uri = workspaces[0].get("workspaceFolderAbsoluteUri") or ""
      if uri:
        ws_label = uri.replace("file://", "").rstrip("/").split("/")[-1] or uri

    items.append({
        "id": cid,
        "conversationId": cid,
        "title": title,
        "status": status,
        "isRunning": status == "RUNNING",
        "stepCount": step_count,
        "steps": step_count,
        "toolCallCount": tmeta.get("toolCallCount", 0),
        "subagentCount": tmeta.get("subagentCount", 0),
        "lastAction": (
            tmeta.get("lastAction") or tmeta.get("lastStepType") or "Idle"
        ),
        "updatedAt": updated_at,
        "createdAt": created_at,
        "workspace": ws_label,
        "isCurrent": cid == active_conv_id,
        "mtime": tmeta.get("mtime", 0.0),
    })

  items.sort(
      key=lambda x: (
          x["updatedAt"] or "",
          x["mtime"],
          x["id"],
      ),
      reverse=True,
  )

  top_items = items[:18]

  global_in = 0
  global_out = 0
  global_think = 0
  global_cache = 0
  global_cost = 0.0
  global_calls = 0

  for idx, item in enumerate(top_items):
    is_active = item["id"] == active_conv_id
    if idx < 6 or is_active:
      tok = _get_token_telemetry(
          item["id"], include_generations=is_active, allow_rpc=is_active
      )
      item["tokens"] = tok
      item["totalTokens"] = tok.get("totalTokens", 0)
      item["estimatedCostUsd"] = tok.get("estimatedCostUsd", 0.0)
      item["cacheHitRatePct"] = tok.get("cacheHitRatePct", 0.0)
      if idx < 6:
        global_in += tok.get("inputTokens", 0)
        global_out += tok.get("outputTokens", 0)
        global_think += tok.get("thinkingTokens", 0)
        global_cache += tok.get("cacheReadTokens", 0)
        global_cost += tok.get("estimatedCostUsd", 0.0)
        global_calls += tok.get("llmCalls", 0)

  global_summary = {
      "sessionsCounted": min(len(top_items), 6),
      "totalConversations": len(items),
      "runningCount": sum(1 for x in items if x["status"] == "RUNNING"),
      "inputTokens": global_in,
      "promptTokens": global_in,
      "outputTokens": global_out,
      "thinkingTokens": global_think,
      "cacheReadTokens": global_cache,
      "cachedTokens": global_cache,
      "totalTokens": global_in + global_out + global_think,
      "cacheHitRatePct": (
          round((global_cache / global_in) * 100.0, 1)
          if global_in > 0
          else 0.0
      ),
      "estimatedCostUsd": round(global_cost, 4),
      "llmCalls": global_calls,
  }
  return top_items, global_summary


def _format_uptime(seconds: int) -> str:
  if seconds <= 0:
    return "0s"
  days, rem = divmod(seconds, 86400)
  hours, rem = divmod(rem, 3600)
  mins, secs = divmod(rem, 60)
  if days > 0:
    return f"{days}d {hours}h"
  if hours > 0:
    return f"{hours}h {mins}m"
  if mins > 0:
    return f"{mins}m {secs}s"
  return f"{secs}s"


def _cron_utc_to_sgt_label(cron_expr: str) -> str:
  """Translates a 5-field UTC cron string into a human-friendly SGT (UTC+8) description."""
  parts = cron_expr.strip().split()
  if len(parts) != 5:
    return cron_expr
  minute, hour, _dom, _month, dow = parts
  dow_label = {
      "*": "Daily",
      "1-5": "Weekdays (Mon–Fri)",
      "0,6": "Weekends",
  }.get(dow, f"DOW {dow}")

  if hour.isdigit() and minute.isdigit():
    sgt_h = (int(hour) + 8) % 24
    return (
        f"{dow_label} at {sgt_h:02d}:{int(minute):02d} SGT"
        f" ({int(hour):02d}:{int(minute):02d} UTC)"
    )
  if hour == "*" and minute.isdigit():
    return f"Hourly at :{int(minute):02d} SGT/UTC ({dow_label})"
  if hour == "22-23,0-11" and minute.isdigit():
    return f"Hourly 06:{int(minute):02d}–19:{int(minute):02d} SGT ({dow_label})"
  return f"{cron_expr} UTC (SGT = UTC+8)"


_IGNORED_STATE_JSON_FILES = frozenset({
    "sidecar.json",
    "plugin.json",
    "package.json",
    "package-lock.json",
    "tsconfig.json",
})


def _inspect_sidecar_state_badge(sdir: str) -> str:
  """Dynamically inspects any local JSON state tracker in a sidecar directory."""
  try:
    for fname in sorted(os.listdir(sdir)):
      if not fname.endswith(".json") or fname in _IGNORED_STATE_JSON_FILES:
        continue
      fpath = os.path.join(sdir, fname)
      if not os.path.isfile(fpath):
        continue
      with open(fpath, "r", encoding="utf-8") as cf:
        cdata = json.load(cf)
      if isinstance(cdata, (list, dict)):
        return f"{len(cdata)} items tracked"
  except Exception:
    pass
  return ""


def _list_automations_and_sidecars(force: bool = False) -> list[dict]:
  """Discovers all configured automations & sidecars and matches live OS processes."""
  now = time.time()
  if (
      not force
      and _AUTOMATIONS_CACHE["data"]
      and (now - _AUTOMATIONS_CACHE["ts"] < 30.0)
  ):
    return _AUTOMATIONS_CACHE["data"]

  procs = []
  try:
    res = subprocess.run(
        ["ps", "-eo", "pid,etimes,args"],
        capture_output=True,
        text=True,
        timeout=3,
    )
    for line in res.stdout.splitlines()[1:]:
      parts = line.strip().split(None, 2)
      if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
        pid_int = int(parts[0])
        proc_cwd = ""
        try:
          proc_cwd = os.path.realpath(f"/proc/{pid_int}/cwd")
        except OSError:
          pass
        procs.append({
            "pid": pid_int,
            "etimes": int(parts[1]),
            "cmd": parts[2],
            "cwd": proc_cwd,
        })
  except Exception:
    pass

  manifests = []
  loose_root = os.path.join(CONFIG_DIR, "sidecars")
  if os.path.isdir(loose_root):
    for name in sorted(os.listdir(loose_root)):
      sjson = os.path.join(loose_root, name, "sidecar.json")
      if os.path.isfile(sjson):
        manifests.append(
            (name, "user-sidecar", os.path.join(loose_root, name), sjson)
        )

  plugins_root = os.path.join(CONFIG_DIR, "plugins")
  if os.path.isdir(plugins_root):
    for pname in sorted(os.listdir(plugins_root)):
      plugin_dir = os.path.join(plugins_root, pname)
      pjson = os.path.join(plugin_dir, "plugin.json")
      declared_name = pname
      if os.path.isfile(pjson):
        try:
          with open(pjson, "r", encoding="utf-8") as pf:
            declared_name = json.load(pf).get("name") or pname
        except Exception:
          pass
      sdir_root = os.path.join(plugin_dir, "sidecars")
      if os.path.isdir(sdir_root):
        for sname in sorted(os.listdir(sdir_root)):
          sjson = os.path.join(sdir_root, sname, "sidecar.json")
          if os.path.isfile(sjson):
            manifests.append((
                f"{declared_name}/{sname}",
                "ui-plugin",
                os.path.join(sdir_root, sname),
                sjson,
            ))

  builtin_root = os.path.join(JETSKI_DIR, "builtin", "plugins")
  if os.path.isdir(builtin_root):
    for pname in sorted(os.listdir(builtin_root)):
      sdir_root = os.path.join(builtin_root, pname, "sidecars")
      if os.path.isdir(sdir_root):
        for sname in sorted(os.listdir(sdir_root)):
          sjson = os.path.join(sdir_root, sname, "sidecar.json")
          if os.path.isfile(sjson):
            manifests.append((
                f"{pname}/{sname}",
                "builtin-sidecar",
                os.path.join(sdir_root, sname),
                sjson,
            ))

  results = []
  for sid, source_type, sdir, sjson_path in manifests:
    try:
      with open(sjson_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    except Exception:
      continue

    builtin = cfg.get("builtin", "")
    cmd_name = cfg.get("command", "")
    args = cfg.get("args") or []
    has_web_ui = bool(cfg.get("has_web_ui"))
    display_name = (
        cfg.get("display_name")
        or (cfg.get("ui_config") or {}).get("title")
        or sid
    )
    description = cfg.get("description") or ""
    restart_policy = cfg.get("restart_policy") or "never"

    kind = (
        "ui-plugin"
        if has_web_ui
        else ("cron-automation" if builtin == "schedule" else "daemon")
    )
    cron_utc = ""
    schedule_sgt = ""
    target_summary = ""

    if builtin == "schedule" and args:
      cron_utc = args[0]
      schedule_sgt = _cron_utc_to_sgt_label(cron_utc)
      if len(args) >= 3 and args[1] == "agentapi":
        subcmd = args[2]
        if subcmd == "send-message" and len(args) >= 4:
          target_summary = f"agentapi send-message → {args[3][:8]}…"
        elif subcmd == "new-conversation":
          target_summary = "agentapi new-conversation (headless)"
        else:
          target_summary = f"agentapi {subcmd}"
      else:
        target_summary = " ".join(args[1:3])
    else:
      target_summary = (
          f"{cmd_name} {' '.join(str(a) for a in args[:2])}".strip()
      )
      if not has_web_ui and cmd_name.startswith("python"):
        cron_utc = "Continuous loop"
        schedule_sgt = "Continuous Daemon Loop"

    real_sdir = os.path.realpath(sdir)
    matched_proc = None
    for p in procs:
      pcmd = p["cmd"]
      pcwd = p.get("cwd", "")
      if builtin == "schedule" and cron_utc:
        if "multicall schedule" in pcmd and cron_utc in pcmd:
          matched_proc = p
          break
      elif pcwd and pcwd == real_sdir:
        matched_proc = p
        break
      elif sdir in pcmd or real_sdir in pcmd:
        matched_proc = p
        break
      elif (
          args
          and len(args) == 1
          and args[0] != "server.py"
          and args[0] in pcmd
      ):
        matched_proc = p
        break
      elif sid == "jetski-harness/console" and p["pid"] == os.getpid():
        matched_proc = p
        break

    extra_badge = _inspect_sidecar_state_badge(sdir)

    prompt_preview = ""
    mechanism_desc = ""
    safety_desc = ""
    if builtin == "schedule" and len(args) >= 3 and args[1] == "agentapi":
      subcmd = args[2]
      raw_prompt = str(args[-1]).strip() if len(args) >= 4 else ""
      prompt_preview = raw_prompt[:360] + ("…" if len(raw_prompt) > 360 else "")
      if subcmd == "send-message" and len(args) >= 4:
        mechanism_desc = (
            f"Scheduled via Jetski's builtin 'schedule' runner ({cron_utc} UTC). "
            "Instead of spawning a new session each run, it dispatches `agentapi send-message` "
            f"to persistent conversation `{args[3][:8]}…` so all runs stay consolidated in one thread."
        )
      elif subcmd == "new-conversation":
        mechanism_desc = (
            f"Scheduled via Jetski's builtin 'schedule' runner ({cron_utc} UTC). "
            "Spawns a fresh headless agent conversation (`agentapi new-conversation`) to execute "
            "the multi-step workflow autonomously."
        )
      else:
        mechanism_desc = (
            f"Scheduled via `builtin: schedule` ({cron_utc} UTC) invoking `{target_summary}`."
        )
      safety_desc = (
          f"Controlled by SidecarManager (`restart_policy: {restart_policy}`). "
          "Can be paused/resumed or triggered on-demand from this dashboard."
      )
    elif has_web_ui:
      mechanism_desc = (
          f"Always-on HTTP UI sidecar (`{target_summary}`, `has_web_ui: true`) "
          "mounted into the Jetski auxiliary pane."
      )
      safety_desc = (
          "Read-only local telemetry inspection and authenticated JSON-RPC bridge."
      )
    else:
      mechanism_desc = (
          f"Background daemon process (`{target_summary}`) managed by SidecarManager "
          f"with `restart_policy: {restart_policy}`."
      )
      safety_desc = (
          f"Deduplicates state locally{f' ({extra_badge})' if extra_badge else ''} "
          "and operates in read-only monitoring mode."
      )

    log_lines = []
    for candidate_id in (sid, sid.replace("/", "_"), sid.split("/")[-1]):
      log_file = os.path.join(
          JETSKI_DIR, "sidecar_data", candidate_id, "logs", "sidecar.log"
      )
      if os.path.isfile(log_file):
        try:
          with open(log_file, "r", encoding="utf-8", errors="replace") as lf:
            lines = [ln.strip() for ln in lf.readlines() if ln.strip()]
            log_lines = lines[-6:]
          break
        except Exception:
          pass

    results.append({
        "id": sid,
        "displayName": display_name,
        "description": description,
        "kind": kind,
        "sourceType": source_type,
        "restartPolicy": restart_policy,
        "hasWebUi": has_web_ui,
        "cronUtc": cron_utc,
        "scheduleSgt": (
            schedule_sgt
            or (
                "Always-On Web UI Sidecar"
                if has_web_ui
                else "Continuous Daemon"
            )
        ),
        "targetSummary": target_summary,
        "promptPreview": prompt_preview,
        "mechanismDesc": mechanism_desc,
        "safetyDesc": safety_desc,
        "isRunning": matched_proc is not None,
        "pid": matched_proc["pid"] if matched_proc else None,
        "uptimeSeconds": matched_proc["etimes"] if matched_proc else 0,
        "uptimeFormatted": (
            _format_uptime(matched_proc["etimes"])
            if matched_proc
            else "Stopped"
        ),
        "extraBadge": extra_badge,
        "recentLogs": log_lines,
        "canTriggerNow": (
            builtin == "schedule" and len(args) >= 3 and args[1] == "agentapi"
        ),
    })

  kind_order = {"cron-automation": 0, "daemon": 1, "ui-plugin": 2}
  results.sort(
      key=lambda r: (
          0 if r["isRunning"] else 1,
          kind_order.get(r["kind"], 3),
          r["id"],
      )
  )
  _AUTOMATIONS_CACHE.update({"ts": now, "data": results})
  return results


def _get_runtime_and_mcp_status(force: bool = False) -> dict:
  """Collects MCP server health, OAuth status, Memory FUSE status, and LS state."""
  now = time.time()
  if (
      not force
      and _RUNTIME_STATUS_CACHE["data"]
      and (now - _RUNTIME_STATUS_CACHE["ts"] < 30.0)
  ):
    return _RUNTIME_STATUS_CACHE["data"]

  oauth_path = os.path.join(JETSKI_DIR, "mcp_oauth_tokens.json")
  oauth_map = {}
  if os.path.isfile(oauth_path):
    try:
      with open(oauth_path, "r", encoding="utf-8") as f:
        raw_oauth = json.load(f)
      if isinstance(raw_oauth, dict):
        oauth_map = (
            raw_oauth.get("tokens")
            if isinstance(raw_oauth.get("tokens"), dict)
            else raw_oauth
        )
    except Exception:
      pass

  mcp_root = os.path.join(JETSKI_DIR, "mcp")
  mcp_servers = []
  if os.path.isdir(mcp_root):
    for sname in sorted(os.listdir(mcp_root)):
      sdir = os.path.join(mcp_root, sname)
      if not os.path.isdir(sdir):
        continue
      tools = [
          fn[:-5]
          for fn in sorted(os.listdir(sdir))
          if fn.endswith(".json") and fn != "mcp_config.json"
      ]
      short_label = sname.replace("_google_", " · ").replace("_", " ")
      mcp_servers.append({
          "name": sname,
          "shortName": (
              sname.split("_google_")[-1] if "_google_" in sname else sname
          ),
          "displayName": short_label.title(),
          "toolCount": len(tools),
          "sampleTools": tools[:5],
          "status": "READY" if tools else "EMPTY",
          "oauthAuthenticated": bool(oauth_map),
      })

  memory_files = []
  if os.path.isdir(MEMORY_DIR):
    for root, _, files in os.walk(MEMORY_DIR):
      for fn in files:
        if fn.endswith(".md"):
          full_p = os.path.join(root, fn)
          rel_p = os.path.relpath(full_p, MEMORY_DIR)
          try:
            mt = os.path.getmtime(full_p)
          except OSError:
            mt = 0.0
          memory_files.append({
              "path": rel_p,
              "updatedAt": (
                  datetime.fromtimestamp(mt, tz=SGT_TZ).strftime(
                      "%Y-%m-%d %H:%M SGT"
                  )
                  if mt
                  else ""
              ),
              "mtime": mt,
          })
  memory_files.sort(key=lambda x: x["mtime"], reverse=True)

  ls_addr, _, ls_pid = _discover_language_server(force=False)
  now_utc = datetime.now(timezone.utc)
  now_sgt = now_utc.astimezone(SGT_TZ)

  res = {
      "mcpServers": mcp_servers,
      "mcpTotalTools": sum(s["toolCount"] for s in mcp_servers),
      "oauthConfigured": bool(oauth_map),
      "oauthGrantsCount": len(oauth_map),
      "memoryMounted": os.path.isdir(MEMORY_DIR),
      "memoryFileCount": len(memory_files),
      "recentMemories": memory_files[:8],
      "languageServer": {
          "online": bool(ls_addr),
          "address": ls_addr or "disconnected",
          "pid": ls_pid,
      },
      "host": {
          "hostname": os.uname().nodename,
          "user": os.environ.get("USER", "user"),
          "timeUtc": now_utc.strftime("%H:%M:%S UTC"),
          "timeSgt": now_sgt.strftime("%Y-%m-%d %H:%M:%S SGT"),
          "sidecarPid": os.getpid(),
          "sidecarPort": int(
              os.environ.get("ANTIGRAVITY_SIDECAR_WEB_PORT", 0)
          ),
      },
  }
  _RUNTIME_STATUS_CACHE.update({"ts": now, "data": res})
  return res


# --- Embedded Agent Tracer Compatibility Helpers ---


def _synthesize_steps_from_ls_trajectory(conv_id: str) -> list[dict]:
  """Fallback: synthesizes transcript.jsonl-compatible steps from Language Server GetCascadeTrajectory."""
  if not conv_id:
    return []
  resp = _call_ls("GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.5)
  if not resp or not isinstance(resp.get("trajectory"), dict):
    return []
  raw_steps = resp["trajectory"].get("steps") or []
  out = []
  for idx, s in enumerate(raw_steps):
    if not isinstance(s, dict):
      continue
    stype = s.get("type") or ""
    meta = s.get("metadata") or {}
    created = meta.get("createdAt") or meta.get("created_at") or ""
    status = (s.get("status") or "DONE").replace("CORTEX_STEP_STATUS_", "")

    if stype == "CORTEX_STEP_TYPE_USER_INPUT":
      ui = s.get("userInput") or {}
      text = ui.get("userResponse") or ""
      if not text and isinstance(ui.get("items"), list):
        text = "".join(
            c.get("text", "") for c in ui["items"] if isinstance(c, dict)
        )
      if text:
        out.append({
            "step_index": idx,
            "type": "USER_INPUT",
            "status": "DONE",
            "created_at": created,
            "content": text,
        })
    elif stype == "CORTEX_STEP_TYPE_PLANNER_RESPONSE":
      pr = s.get("plannerResponse") or {}
      content = pr.get("modifiedResponse") or pr.get("response") or ""
      thinking = pr.get("thinking") or ""
      tcalls = []
      for raw_tc in pr.get("toolCalls") or []:
        tc = (
            raw_tc.get("toolCall", raw_tc) if isinstance(raw_tc, dict) else {}
        )
        tname = tc.get("name") or "tool"
        args_raw = tc.get("argumentsJson") or tc.get("arguments") or {}
        if isinstance(args_raw, str):
          try:
            args_raw = json.loads(args_raw)
          except Exception:
            args_raw = {"raw": args_raw}
        tcalls.append({
            "id": tc.get("id") or f"tc_{idx}_{len(tcalls)}",
            "name": tname,
            "args": args_raw,
            "arguments": args_raw,
        })
      out.append({
          "step_index": idx,
          "type": "PLANNER_RESPONSE",
          "status": "DONE" if status in ("DONE", "COMPLETED") else status,
          "created_at": created,
          "content": content,
          "thinking": thinking,
          "tool_calls": tcalls,
      })
  return out


def _read_transcript_steps(conv_id: str, since: int = -1) -> list[dict]:
  """Reads parsed transcript steps from the shared incremental step cache."""
  bundle = _get_cached_transcript_bundle(conv_id)
  steps = bundle["steps"]
  if steps:
    if since < 0:
      return steps
    return [s for s in steps if s.get("step_index", -1) > since]
  if since <= -1:
    return [
        s
        for s in _synthesize_steps_from_ls_trajectory(conv_id)
        if s.get("step_index", -1) > since
    ]
  return []


def _read_step_full(conv_id: str, step_idx: int) -> dict | None:
  cache_key = (conv_id, step_idx)
  cached_step = _lru_get(_FULL_STEP_CONTENT_CACHE, cache_key)
  if cached_step is not None:
    return cached_step

  for full_flag in (True, False):
    tpath = _get_transcript_path(conv_id, full=full_flag)
    if not tpath or not os.path.isfile(tpath):
      continue
    try:
      with open(tpath, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
          line = raw.strip()
          if not line:
            continue
          try:
            step = json.loads(line)
            if step.get("step_index") == step_idx:
              if step.get("status") == "DONE":
                _lru_set(_FULL_STEP_CONTENT_CACHE, cache_key, step, max_size=256)
              return step
          except json.JSONDecodeError:
            continue
    except OSError:
      pass
  return None


def _get_subagents_live_status(sub_ids: list[str]) -> dict:
  """Returns live status for child subagent conversations requested by embedded /tracer."""
  out = {}
  for cid in sub_ids[:25]:
    if not cid or not _UUID_RE.match(cid):
      continue
    tmeta = _summarize_transcript_fast(cid)
    if not tmeta.get("exists"):
      out[cid] = {"exists": False, "state": "unknown"}
      continue
    status = tmeta.get("status", "IDLE")
    state_label = (
        "running"
        if status == "RUNNING"
        else ("errored" if status == "ERROR" else "completed")
    )
    out[cid] = {
        "exists": True,
        "state": state_label,
        "steps": tmeta.get("stepCount", 0),
        "tool_calls": tmeta.get("toolCallCount", 0),
        "last_tool": tmeta.get("lastAction") or "",
        "age_seconds": round(
            max(0.0, time.time() - (tmeta.get("mtime") or time.time())), 1
        ),
    }
  return out


def _check_tracer_git_update(force: bool = False) -> dict:
  now = time.time()
  if (
      not force
      and _UPDATE_CACHE["data"]
      and (now - _UPDATE_CACHE["ts"] < _UPDATE_TTL)
  ):
    return _UPDATE_CACHE["data"]
  try:
    subprocess.run(
        ["git", "-C", TRACER_DIR, "fetch", "--quiet"],
        timeout=10,
        check=True,
        capture_output=True,
    )
    local = subprocess.check_output(
        ["git", "-C", TRACER_DIR, "rev-parse", "HEAD"], text=True, timeout=5
    ).strip()
    remote = subprocess.check_output(
        ["git", "-C", TRACER_DIR, "rev-parse", "@{u}"], text=True, timeout=5
    ).strip()
    behind = int(
        subprocess.check_output(
            ["git", "-C", TRACER_DIR, "rev-list", "--count", "HEAD..@{u}"],
            text=True,
            timeout=5,
        ).strip()
    )
    result = {
        "supported": True,
        "update_available": behind > 0,
        "behind": behind,
        "local": local[:7],
        "local_sha": local[:7],
        "remote": remote[:7],
        "remote_sha": remote[:7],
        "dirty": False,
        "commits": [],
    }
  except Exception as e:
    result = {
        "supported": False,
        "update_available": False,
        "behind": 0,
        "error": str(e),
    }
  _UPDATE_CACHE["data"] = result
  _UPDATE_CACHE["ts"] = now
  return result


def _render_embedded_tracer_html(conv_id: str) -> bytes:
  """Loads Agent Tracer index.html from memory cache and injects conversation-sync & compact-mode bridge."""
  tracer_html_path = os.path.join(TRACER_DIR, "index.html")
  raw_bytes = _read_cached_file_bytes(tracer_html_path)
  if raw_bytes is None:
    return b"<html><body style='background:#0d1117;color:#e6edf3;font-family:sans-serif;padding:24px'>Agent Tracer index.html not found.</body></html>"

  html = raw_bytes.decode("utf-8", errors="replace")

  bridge_script = f"""
<style>
  html.harness-compact-embed .header,
  html.harness-compact-embed .goalbar,
  html.harness-compact-embed #right,
  html.harness-compact-embed #right-bar,
  html.harness-compact-embed .topo-footer {{
    display: none !important;
  }}
  html.harness-compact-embed #left {{
    flex: 1 1 100% !important;
    width: 100% !important;
    height: 100% !important;
    border-right: none !important;
  }}
  html.harness-compact-embed .swimlane-header {{
    font-size: 10.5px !important;
    padding: 3px 7px !important;
  }}
  html.harness-compact-embed .topo-hud {{
    flex-wrap: nowrap !important;
    padding: 3px 8px !important;
    min-height: 26px !important;
    max-height: 28px !important;
    gap: 6px !important;
    overflow: hidden !important;
  }}
  html.harness-compact-embed .topo-hud-body {{
    min-width: 0 !important;
  }}
  html.harness-compact-embed .topo-hud-inspect {{
    display: none !important;
  }}
  html.harness-compact-embed .topo-stage {{
    flex: 1 1 auto !important;
    min-height: 140px !important;
    height: 100% !important;
  }}
</style>
<script>
  (function() {{
    const urlParams = new URLSearchParams(window.location.search);
    const forcedConvId = urlParams.get('conversationId') || {json.dumps(conv_id or "")};
    const uiToken = urlParams.get('token') || "";
    const isCompact = urlParams.get('compact') === '1';
    const initialMode = urlParams.get('mode') || "";
    if (isCompact) {{
      document.documentElement.classList.add('harness-compact-embed');
    }}
    if (!window.sidecar) {{
      window.sidecar = {{}};
    }}
    if (forcedConvId) {{
      window.sidecar.conversationId = forcedConvId;
    }}
    if (uiToken && window.fetch) {{
      const origFetch = window.fetch.bind(window);
      window.fetch = function(resource, init) {{
        const opts = Object.assign({{}}, init || {{}});
        const headers = new Headers(opts.headers || {{}});
        if (!headers.has('X-Sidecar-Token')) {{
          headers.set('X-Sidecar-Token', uiToken);
        }}
        opts.headers = headers;
        return origFetch(resource, opts);
      }};
    }}
    window.addEventListener('message', function(ev) {{
      if (!ev.data) return;
      if (ev.data.type === 'HARNESS_SET_CONVERSATION' && ev.data.conversationId) {{
        if (window.sidecar.conversationId !== ev.data.conversationId) {{
          const nextUrl = new URL(window.location.href);
          nextUrl.searchParams.set('conversationId', ev.data.conversationId);
          window.location.replace(nextUrl.toString());
        }}
      }} else if (ev.data.type === 'HARNESS_SET_THEME') {{
        const wantDark = ev.data.theme ? (ev.data.theme === 'dark') : !!ev.data.dark;
        const curDark = document.documentElement.getAttribute('data-theme') === 'dark';
        if (wantDark !== curDark) {{
          const btn = document.getElementById('btnTheme');
          if (btn) btn.click();
          else document.documentElement.setAttribute('data-theme', wantDark ? 'dark' : 'light');
        }}
      }} else if (ev.data.type === 'HARNESS_SET_VIEW_MODE' && ev.data.mode) {{
        const btnId = ev.data.mode === 'topology' ? 'btnViewTopology' : 'btnViewTimeline';
        const btn = document.getElementById(btnId);
        if (btn) btn.click();
      }} else if (ev.data.type === 'HARNESS_FOCUS_STEP' && ev.data.stepIndex != null) {{
        const timelineBtn = document.getElementById('btnViewTimeline');
        if (timelineBtn && document.body.classList.contains('topology-mode')) {{
          timelineBtn.click();
        }}
        setTimeout(function() {{
          const searchInput = document.getElementById('searchBox');
          if (ev.data.toolName && searchInput) {{
            searchInput.value = ev.data.toolName;
            searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
          }}
        }}, 60);
      }}
    }});
    window.addEventListener('DOMContentLoaded', function() {{
      if (isCompact && document.body) {{
        document.body.classList.add('inspector-collapsed');
      }}
      if (initialMode === 'topology') {{
        setTimeout(function() {{
          const b = document.getElementById('btnViewTopology');
          if (b) b.click();
        }}, 120);
      }}
      const btn = document.getElementById('btnTheme');
      if (btn) {{
        btn.addEventListener('click', function() {{
          setTimeout(function() {{
            const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
            try {{
              window.parent.postMessage({{ type: 'TRACER_THEME_CHANGED', dark: isDark, theme: isDark ? 'dark' : 'light' }}, '*');
            }} catch (e) {{}}
          }}, 20);
        }});
      }}
    }});
  }})();
</script>
"""
  if "</head>" in html:
    html = html.replace("</head>", bridge_script + "\n</head>", 1)
  else:
    html = bridge_script + html
  return html.encode("utf-8")


# --- Structured Chat Stream & Direct Connect-RPC Dispatch ---


def _clean_user_input(raw_content: str) -> tuple[str, str]:
  """Extracts clean user prompt and optional system notice from USER_INPUT step."""
  if not raw_content:
    return "", ""
  sys_notice = ""
  sm = re.search(
      r"<SYSTEM_MESSAGE>(.*?)</SYSTEM_MESSAGE>", raw_content, re.DOTALL
  )
  if sm:
    sys_notice = sm.group(1).strip()

  um = re.search(r"<USER_REQUEST>(.*?)</USER_REQUEST>", raw_content, re.DOTALL)
  if um:
    return um.group(1).strip(), sys_notice

  cleaned = re.sub(
      r"<ADDITIONAL_METADATA>.*?</ADDITIONAL_METADATA>",
      "",
      raw_content,
      flags=re.DOTALL,
  )
  cleaned = re.sub(
      r"<SYSTEM_MESSAGE>.*?</SYSTEM_MESSAGE>", "", cleaned, flags=re.DOTALL
  )
  cleaned = re.sub(
      r"<CONTEXT_SUMMARY>.*?</CONTEXT_SUMMARY>", "", cleaned, flags=re.DOTALL
  )
  return cleaned.strip(), sys_notice


def _summarize_thinking_line(thinking: str) -> str:
  """Extracts a clean 1-line summary from model thinking text."""
  if not thinking:
    return ""
  lines = [ln.strip() for ln in thinking.strip().splitlines() if ln.strip()]
  if not lines:
    return ""
  first = re.sub(r"^[\*\#\-\s]+|[\*\#\s]+$", "", lines[0]).strip()
  if (
      len(lines) > 1
      and len(first) < 55
      and not first.endswith((".", "!", "?", ":"))
  ):
    second = re.sub(r"^[\*\#\-\s]+", "", lines[1]).strip()
    combined = f"{first} — {second}"
    return combined[:110] + ("…" if len(combined) > 110 else "")
  return first[:110] + ("…" if len(first) > 110 else "")


def _parse_tool_result_step(res_step: dict) -> dict:
  """Parses a paired GENERIC tool result step for duration, status, and body."""
  raw = res_step.get("content") or ""
  status = res_step.get("status") or "DONE"
  created_iso = ""
  completed_iso = ""
  body_lines = []
  in_preamble = True

  for line in raw.splitlines():
    if in_preamble and line.startswith("Created At:"):
      created_iso = line.split("Created At:", 1)[1].strip()
      continue
    if in_preamble and line.startswith("Completed At:"):
      completed_iso = line.split("Completed At:", 1)[1].strip()
      continue
    if in_preamble and not line.strip():
      in_preamble = False
      continue
    in_preamble = False
    body_lines.append(line)

  body = "\n".join(body_lines).strip()
  duration_ms = None
  if created_iso and completed_iso:
    try:
      dt1 = datetime.fromisoformat(created_iso.replace("Z", "+00:00"))
      dt2 = datetime.fromisoformat(completed_iso.replace("Z", "+00:00"))
      duration_ms = max(0, int((dt2 - dt1).total_seconds() * 1000))
    except Exception:
      pass

  is_error = status == "ERROR"
  if not is_error:
    header_part = (
        body.split("Output:", 1)[0] if "Output:" in body else body[:400]
    )
    if re.search(r"The command exited with code [1-9]\d*", header_part):
      is_error = True
    elif body.startswith("Encountered error in tool execution:"):
      is_error = True

  return {
      "resultStepIndex": res_step.get("step_index"),
      "status": "ERROR" if is_error else status,
      "durationMs": duration_ms,
      "outputPreview": body[:1200],
      "isTruncated": len(body) > 1200 or bool(res_step.get("truncated_fields")),
  }


def _clean_arg_value(val):
  """Recursively strips double-encoded surrounding quotes from compact transcript tool args."""
  if isinstance(val, str):
    s = val.strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
      s = s[1:-1].strip()
    return s
  if isinstance(val, dict):
    return {k: _clean_arg_value(v) for k, v in val.items()}
  if isinstance(val, list):
    return [_clean_arg_value(x) for x in val]
  return val


def _extract_tool_target(name: str, args: dict) -> str:
  if not isinstance(args, dict):
    return ""
  for key in ("AbsolutePath", "TargetFile", "NotebookPath", "Url", "Query"):
    val = _clean_arg_value(args.get(key))
    if val and isinstance(val, str):
      if key in ("AbsolutePath", "TargetFile", "NotebookPath"):
        parts = val.rstrip("/").split("/")
        return "/".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
      return val[:70]
  if name == "run_command" and args.get("CommandLine"):
    cmd = _clean_arg_value(str(args["CommandLine"])).splitlines()[0]
    return cmd[:72] + ("…" if len(cmd) > 72 else "")
  if name == "call_mcp_tool":
    return (
        f"{_clean_arg_value(args.get('ServerName', ''))} ·"
        f" {_clean_arg_value(args.get('ToolName', ''))}".strip(" ·")
    )
  if name == "invoke_subagent":
    subs = args.get("Subagents") or []
    if subs and isinstance(subs, list) and isinstance(subs[0], dict):
      return f"{_clean_arg_value(subs[0].get('Role') or subs[0].get('TypeName') or 'Subagent')}"
  return ""


def _build_chat_stream(conv_id: str) -> dict:
  """Builds structured chat messages + paired tool calls + subagents feed for the primary Chat UI."""
  if not conv_id:
    return {"conversationId": "", "items": [], "subagents": [], "stepCount": 0}

  bundle = _get_cached_transcript_bundle(conv_id)
  mtime_ns = bundle["mtime_ns"]
  fsize = bundle["size"]

  cached = _lru_get(_CHAT_STREAM_CACHE, conv_id)
  if cached and mtime_ns > 0 and cached[0] == mtime_ns and cached[1] == fsize:
    return cached[2]

  steps = _read_transcript_steps(conv_id, since=-1)
  tok_info = _get_token_telemetry(conv_id, include_generations=True)
  gen_by_step = {}
  for g in tok_info.get("generations") or []:
    if g.get("stepIndex") is not None:
      gen_by_step[g["stepIndex"]] = g

  items = []
  subagents = []
  n = len(steps)
  i = 0
  while i < n:
    st = steps[i]
    stype = st.get("type", "")
    sidx = st.get("step_index", i)
    created = st.get("created_at", "")

    if stype == "USER_INPUT":
      user_text, sys_notice = _clean_user_input(st.get("content") or "")
      if user_text or sys_notice:
        items.append({
            "kind": "user",
            "role": "user",
            "stepIndex": sidx,
            "lastStepIndex": sidx,
            "createdAt": created,
            "content": user_text,
            "systemNotice": sys_notice,
        })
      i += 1
      continue

    if stype == "PLANNER_RESPONSE":
      trunc_fields = st.get("truncated_fields") or []
      if "content" in trunc_fields:
        full_st = _read_step_full(conv_id, sidx)
        if full_st and full_st.get("content"):
          st = {**st, "content": full_st["content"]}
      thinking = (st.get("thinking") or "").strip()
      content = (st.get("content") or "").strip()
      raw_tcalls = st.get("tool_calls") or []

      paired_tools = []
      j = i + 1
      for tc in raw_tcalls:
        tname = tc.get("name") or "tool"
        targs = tc.get("arguments") or tc.get("args") or {}
        if isinstance(targs, str):
          try:
            targs = json.loads(targs)
          except Exception:
            targs = {"raw": targs}
        targs = (
            _clean_arg_value(targs)
            if isinstance(targs, dict)
            else {"raw": str(targs)}
        )
        action_label = (
            (targs.get("toolAction") if isinstance(targs, dict) else None)
            or (targs.get("toolSummary") if isinstance(targs, dict) else None)
            or tname
        )
        target_hint = _extract_tool_target(tname, targs)

        res_meta = {
            "resultStepIndex": None,
            "status": "RUNNING" if st.get("status") != "DONE" else "DONE",
            "durationMs": None,
            "outputPreview": "",
            "isTruncated": False,
        }
        if j < n and steps[j].get("type") == "GENERIC":
          res_meta = _parse_tool_result_step(steps[j])
          j += 1

        if tname == "invoke_subagent" and isinstance(targs, dict):
          for sub_entry in targs.get("Subagents") or []:
            if isinstance(sub_entry, dict):
              subagents.append({
                  "stepIndex": sidx,
                  "role": (
                      sub_entry.get("Role")
                      or sub_entry.get("TypeName")
                      or "Subagent"
                  ),
                  "typeName": sub_entry.get("TypeName") or "self",
                  "promptPreview": (sub_entry.get("Prompt") or "")[:140],
                  "status": res_meta["status"],
                  "durationMs": res_meta["durationMs"],
                  "createdAt": created,
              })

        try:
          args_preview = (
              json.dumps(targs, indent=2)
              if isinstance(targs, dict)
              else str(targs)
          )[:600]
        except Exception:
          args_preview = str(targs)[:600]

        paired_tools.append({
            "id": tc.get("id") or f"tc_{sidx}_{len(paired_tools)}",
            "stepIndex": sidx,
            "name": tname,
            "action": action_label,
            "target": target_hint,
            "summary": action_label or target_hint or "",
            "argsPreview": args_preview,
            **res_meta,
        })

      latest_tool_desc = ""
      if paired_tools:
        lt = paired_tools[-1]
        latest_tool_desc = (
            f"{lt['action']} ({lt['target']})"
            if lt.get("target")
            and lt.get("action")
            and lt["target"] not in lt["action"]
            else (lt.get("action") or lt.get("target") or lt["name"])
        )

      gen_stat = gen_by_step.get(sidx)
      if items and items[-1].get("kind") == "assistant":
        prev = items[-1]
        prev["lastStepIndex"] = sidx
        prev["updatedAt"] = created
        prev["status"] = st.get("status") or prev["status"]
        if thinking:
          combined_thinking = (
              f"{prev['thinking']}\n\n---\n\n{thinking}"
              if prev.get("thinking")
              else thinking
          )
          prev["thinking"] = combined_thinking[-3600:]
          prev["thinkingSummary"] = _summarize_thinking_line(thinking)
        if latest_tool_desc:
          prev["latestAction"] = latest_tool_desc
        elif thinking:
          prev["latestAction"] = _summarize_thinking_line(thinking)
        if content:
          prev["content"] = (
              f"{prev['content']}\n\n{content}"
              if prev.get("content")
              else content
          )
        prev["toolCalls"].extend(paired_tools)
        if gen_stat:
          if prev.get("tokenTurn") and isinstance(prev["tokenTurn"], dict):
            prev["tokenTurn"] = {
                **gen_stat,
                "outputTokens": (prev["tokenTurn"].get("outputTokens") or 0)
                + (gen_stat.get("outputTokens") or 0),
            }
          else:
            prev["tokenTurn"] = gen_stat
      else:
        items.append({
            "kind": "assistant",
            "role": "agent",
            "stepIndex": sidx,
            "lastStepIndex": sidx,
            "createdAt": created,
            "updatedAt": created,
            "status": st.get("status") or "DONE",
            "thinking": thinking[-3600:],
            "thinkingSummary": _summarize_thinking_line(thinking),
            "latestAction": (
                latest_tool_desc or _summarize_thinking_line(thinking)
            ),
            "thinkingTruncated": "thinking" in trunc_fields,
            "content": content,
            "toolCalls": paired_tools,
            "tokenTurn": gen_stat,
        })
      i = j
      continue

    if stype == "ERROR_MESSAGE":
      err_content = (st.get("content") or "Agent error occurred").strip()
      if (
          "plan model not specified" in err_content
          or "The stream was interrupted" in err_content
      ):
        i += 1
        continue
      items.append({
          "kind": "error",
          "role": "agent",
          "stepIndex": sidx,
          "lastStepIndex": sidx,
          "createdAt": created,
          "content": err_content,
          "toolCalls": [],
      })
    i += 1

  result = {
      "conversationId": conv_id,
      "stepCount": len(steps),
      "items": items[-120:],
      "subagents": subagents[-20:],
  }
  _lru_set(_CHAT_STREAM_CACHE, conv_id, (mtime_ns, fsize, result))
  return result


def _run_agentapi(args: list[str], project_id: str | None = None) -> dict:
  env = os.environ.copy()
  if project_id:
    env["ANTIGRAVITY_PROJECT_ID"] = project_id
  res = subprocess.run(
      ["agentapi"] + args,
      capture_output=True,
      text=True,
      env=env,
      timeout=25,
  )
  if res.returncode != 0:
    raise RuntimeError(
        res.stderr.strip() or f"agentapi exited with {res.returncode}"
    )
  out = res.stdout.strip()
  try:
    return json.loads(out) if out else {"ok": True}
  except json.JSONDecodeError:
    return {"ok": True, "raw": out}


def _resolve_plan_model(conv_id: str = "", model_tier: str = "pro") -> str:
  """Resolves a valid planModel enum for SendUserCascadeMessage / StartCascade."""
  if conv_id:
    cached = _lru_get(_TOKEN_USAGE_CACHE, conv_id)
    if cached and isinstance(cached[2], dict):
      m = cached[2].get("rawModel") or cached[2].get("model") or ""
      if m.startswith("MODEL_"):
        return m
    traj_resp = _call_ls(
        "GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.0
    )
    if traj_resp and isinstance(traj_resp.get("trajectory"), dict):
      gen_meta = traj_resp["trajectory"].get("generatorMetadata") or []
      for gm in reversed(gen_meta):
        cm = gm.get("chatModel") or {}
        usage = cm.get("usage") or {}
        m = cm.get("model") or usage.get("model") or ""
        if m.startswith("MODEL_"):
          return m

  cfg_resp = _call_ls("GetCascadeModelConfigData", {}, timeout=2.0)
  if cfg_resp and isinstance(cfg_resp, dict):
    m = (
        (cfg_resp.get("defaultOverrideModelConfig") or {})
        .get("modelOrAlias", {})
        .get("model")
        or ""
    )
    if m.startswith("MODEL_"):
      return m

  models_resp = _call_ls("GetAvailableModels", {}, timeout=2.5)
  if models_resp and isinstance(models_resp.get("response"), dict):
    rm = models_resp["response"]
    tiered = rm.get("tieredModelIds") or {}
    models_map = rm.get("models") or {}
    tier_key = (
        "pro"
        if model_tier == "pro"
        else ("flashLite" if model_tier == "flash_lite" else "flash")
    )
    tier_list = (
        tiered.get(tier_key)
        or tiered.get("pro")
        or tiered.get("flash")
        or []
    )
    if tier_list:
      mid = tier_list[0]
      m_info = models_map.get(mid) or {}
      m = m_info.get("model") or ""
      if m.startswith("MODEL_"):
        return m

  return "MODEL_PLACEHOLDER_M37"


def _send_message_direct(
    conv_id: str,
    message: str,
    title: str = "",
    project_id: str | None = None,
) -> dict:
  """Sends a user message to conv_id via Language Server Connect-RPC with automatic model resolution."""
  resolved_model = _resolve_plan_model(conv_id=conv_id)
  rpc_req = {
      "cascadeId": conv_id,
      "items": [{"text": message}],
      "blocking": False,
      "messageOrigin": "AGENT_MESSAGE_ORIGIN_IDE",
      "cascadeConfig": {
          "plannerConfig": {
              "planModel": resolved_model,
              "conversational": {
                  "plannerMode": "CONVERSATIONAL_PLANNER_MODE_DEFAULT",
              },
          }
      },
  }
  resp = _call_ls("SendUserCascadeMessage", rpc_req, timeout=4.0)
  if resp is not None:
    return {
        "ok": True,
        "method": "SendUserCascadeMessage",
        "conversationId": conv_id,
        "model": resolved_model,
        "response": resp,
    }

  agent_msg_req = {
      "recipient": conv_id,
      "content": message,
  }
  if title:
    agent_msg_req["displayTitle"] = title
  resp2 = _call_ls("SendAgentMessage", agent_msg_req, timeout=4.0)
  if resp2 is not None:
    return {
        "ok": True,
        "method": "SendAgentMessage",
        "conversationId": conv_id,
        "response": resp2,
    }

  args = ["send-message"]
  if title:
    args.extend(["--title", title])
  args.extend([conv_id, message])
  return _run_agentapi(args, project_id=project_id)


def _start_conversation_direct(
    message: str,
    title: str = "",
    model_tier: str = "pro",
    project_id: str | None = None,
) -> dict:
  """Starts a new conversation via Language Server Connect-RPC (StartCascade + SendUserCascadeMessage)."""
  resolved_model = _resolve_plan_model(conv_id="", model_tier=model_tier)

  start_req = {
      "source": "CORTEX_TRAJECTORY_SOURCE_CASCADE_CLIENT",
      "trajectoryType": "CORTEX_TRAJECTORY_TYPE_CASCADE",
      "customAgentSpec": {
          "codingAgent": {"googleMode": True},
          "commandExecutionPolicy": "eager",
          "enforcedWorkspaceValidation": False,
          "cascadeConfig": {
              "plannerConfig": {
                  "planModel": resolved_model,
                  "conversational": {
                      "plannerMode": "CONVERSATIONAL_PLANNER_MODE_DEFAULT",
                  },
              }
          },
      },
      "projectEnvConfig": {
          "projectId": (
              project_id or os.environ.get("ANTIGRAVITY_PROJECT_ID", "")
          ),
          "defaultProjectEnvironment": {},
      },
  }

  start_resp = _call_ls("StartCascade", start_req, timeout=4.0)
  if start_resp and start_resp.get("cascadeId"):
    cid = start_resp["cascadeId"]
    if title:
      _call_ls(
          "UpdateConversationAnnotations",
          {
              "cascadeIds": [cid],
              "annotations": {"title": title},
              "mergeAnnotations": True,
          },
          timeout=2.5,
      )
    _call_ls(
        "SendUserCascadeMessage",
        {
            "cascadeId": cid,
            "items": [{"text": message}],
            "blocking": False,
            "messageOrigin": "AGENT_MESSAGE_ORIGIN_IDE",
            "cascadeConfig": {
                "plannerConfig": {
                    "planModel": resolved_model,
                    "conversational": {
                        "plannerMode": "CONVERSATIONAL_PLANNER_MODE_DEFAULT",
                    },
                }
            },
        },
        timeout=4.0,
    )
    return {
        "ok": True,
        "conversationId": cid,
        "model": resolved_model,
        "method": "StartCascade",
    }

  args = ["new-conversation"]
  if title:
    args.extend(["--title", title])
  args.extend(["--", message])
  return _run_agentapi(args, project_id=project_id)


def _trigger_automation_by_id(sidecar_id: str) -> tuple[dict, int]:
  """Triggers a scheduled sidecar automation immediately via Connect-RPC."""
  if not sidecar_id or "/" in sidecar_id or ".." in sidecar_id:
    return {"ok": False, "error": "Invalid automation ID"}, 400
  sjson_path = os.path.join(CONFIG_DIR, "sidecars", sidecar_id, "sidecar.json")
  if not os.path.isfile(sjson_path):
    return {"ok": False, "error": f"Automation {sidecar_id} not found"}, 404
  try:
    with open(sjson_path, "r", encoding="utf-8") as f:
      cfg = json.load(f)
    args = cfg.get("args") or []
    if len(args) >= 5 and args[1] == "agentapi" and args[2] == "send-message":
      return _send_message_direct(args[3], args[4]), 200
    if (
        len(args) >= 4
        and args[1] == "agentapi"
        and args[2] == "new-conversation"
    ):
      return _start_conversation_direct(args[-1], title=sidecar_id), 200
    return {"ok": False, "error": "Unsupported automation command shape"}, 400
  except Exception as e:
    return {"ok": False, "error": str(e)}, 500


def _resolve_request_conv_id(params: dict | None = None, body: dict | None = None) -> str:
  """Resolves conversation ID from query params, POST body, or sidecar environment."""
  if params:
    for key in ("convId", "conversationId", "conv_id"):
      val = (params.get(key) or [None])[0]
      if val:
        return val.strip()
  if body:
    for key in ("convId", "conversationId", "conv_id"):
      val = body.get(key)
      if isinstance(val, str) and val.strip():
        return val.strip()
  return (
      os.environ.get("ANTIGRAVITY_SIDECAR_CONVERSATION_ID")
      or os.environ.get("ANTIGRAVITY_CONVERSATION_ID")
      or ""
  ).strip()


class HarnessRequestHandler(BaseHTTPRequestHandler):
  """HTTP handler for the Custom Jetski Harness & embedded Agent Tracer."""

  def log_message(self, fmt, *args):
    pass

  def _send_bytes(self, body: bytes, content_type: str, status: int = 200):
    self.send_response(status)
    self.send_header("Content-Type", content_type)
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Cache-Control", "no-store")
    self.send_header("Access-Control-Allow-Origin", "*")
    self.end_headers()
    self.wfile.write(body)

  def _send_json(self, data: dict | list, status: int = 200):
    body = json.dumps(data, separators=(",", ":")).encode("utf-8")
    self._send_bytes(body, "application/json; charset=utf-8", status=status)

  def _serve_static(self, filename: str, content_type: str):
    fpath = os.path.join(BASE_DIR, filename)
    body = _read_cached_file_bytes(fpath)
    if body is None:
      self._send_json({"error": f"{filename} not found"}, status=404)
      return
    self._send_bytes(body, content_type)

  def do_GET(self):
    parsed = urllib.parse.urlparse(self.path)
    path = parsed.path
    params = urllib.parse.parse_qs(parsed.query)

    if path in ("/", "/index.html", "/fullscreen"):
      self._serve_static("index.html", "text/html; charset=utf-8")
      return
    if path == "/styles.css":
      self._serve_static("styles.css", "text/css; charset=utf-8")
      return
    if path == "/app.js":
      self._serve_static("app.js", "application/javascript; charset=utf-8")
      return
    if path == "/preload.js":
      body = _read_cached_file_bytes(PRELOAD_SDK_PATH)
      if body is not None:
        self._send_bytes(body, "application/javascript; charset=utf-8")
      else:
        self._send_bytes(
            b"window.sidecar = window.sidecar || {};",
            "application/javascript; charset=utf-8",
        )
      return

    # Embedded Agent Tracer view
    if path == "/tracer":
      conv_id = _resolve_request_conv_id(params=params)
      self._send_bytes(
          _render_embedded_tracer_html(conv_id), "text/html; charset=utf-8"
      )
      return

    # Embedded Agent Tracer API endpoints
    if path == "/api/transcript":
      conv_id = _resolve_request_conv_id(params=params)
      if not conv_id:
        self._send_json([])
        return
      try:
        since = int(params.get("since", ["-1"])[0])
      except ValueError:
        since = -1
      self._send_json(_read_transcript_steps(conv_id, since=since))
      return

    if path == "/api/step_full":
      conv_id = _resolve_request_conv_id(params=params)
      step_idx = params.get("step", [None])[0]
      if not conv_id or step_idx is None:
        self._send_json({}, status=400)
        return
      try:
        step = _read_step_full(conv_id, int(step_idx))
      except ValueError:
        step = None
      self._send_json(step if step else {})
      return

    if path == "/api/subagents_status":
      raw_ids = params.get("ids", [""])[0]
      sub_ids = [x.strip() for x in raw_ids.split(",") if x.strip()]
      self._send_json(_get_subagents_live_status(sub_ids))
      return

    if path == "/api/telemetry":
      conv_id = _resolve_request_conv_id(params=params)
      if not conv_id:
        self._send_json({"available": False, "reason": "No conversationId"})
        return
      tok = _get_token_telemetry(conv_id, include_generations=True)
      self._send_json({
          "available": True,
          "conversationId": conv_id,
          "status": tok.get("lsStatus") or "IDLE",
          "model": tok.get("model") or "Gemini Next",
          "llmCalls": tok.get("llmCalls", 0),
          "totalInputTokens": tok.get("inputTokens", 0),
          "totalOutputTokens": tok.get("outputTokens", 0),
          "totalThinkingTokens": tok.get("thinkingTokens", 0),
          "totalCacheReadTokens": tok.get("cacheReadTokens", 0),
          "totalTokens": tok.get("totalTokens", 0),
          "cacheHitPct": tok.get("cacheHitRatePct", 0.0),
          "estCostUsd": tok.get("estimatedCostUsd", 0.0),
          "turns": tok.get("generations") or [],
      })
      return

    if path == "/api/update-status":
      force = params.get("force", ["0"])[0] == "1"
      self._send_json(_check_tracer_git_update(force=force))
      return

    if path == "/api/conversations":
      host_cid = _resolve_request_conv_id(params=params)
      convs, _ = _list_conversations(host_cid)
      self._send_json({
          "conversations": convs,
          "hostConversationId": host_cid,
      })
      return

    # Structured Chat Stream API for the Primary Chat Canvas
    if path in ("/api/harness/chat", "/api/chat_stream"):
      conv_id = _resolve_request_conv_id(params=params)
      if not conv_id:
        self._send_json({"conversationId": "", "items": [], "stepCount": 0})
        return
      self._send_json(_build_chat_stream(conv_id))
      return

    # Unified Custom Harness Telemetry & Operations API
    if path in ("/api/harness/overview", "/api/state"):
      host_conv_id = (
          os.environ.get("ANTIGRAVITY_SIDECAR_CONVERSATION_ID")
          or os.environ.get("ANTIGRAVITY_CONVERSATION_ID")
          or ""
      )
      req_conv = _resolve_request_conv_id(params=params)
      conversations, global_tokens = _list_conversations(req_conv)
      active_conv_id = req_conv
      if not active_conv_id and conversations:
        active_conv_id = conversations[0]["id"]

      token_telemetry = (
          _get_token_telemetry(active_conv_id, include_generations=True)
          if active_conv_id
          else {}
      )
      active_conv_obj = None
      for c in conversations:
        if c["id"] == active_conv_id:
          c["tokens"] = token_telemetry
          active_conv_obj = c

      chat_stream = (
          _build_chat_stream(active_conv_id)
          if active_conv_id
          else {"conversationId": "", "items": [], "stepCount": 0}
      )
      all_services = _list_automations_and_sidecars()
      runtime = _get_runtime_and_mcp_status()

      cron_items = []
      sidecar_items = []
      for svc in all_services:
        if svc.get("kind") in ("cron-automation", "daemon") and not svc.get(
            "hasWebUi"
        ):
          is_paused = svc.get("restartPolicy") == "never" and not svc.get(
              "isRunning"
          )
          cron_items.append({
              **svc,
              "plugin": svc.get("id", ""),
              "name": svc.get("displayName") or svc.get("id", ""),
              "cron": (
                  svc.get("scheduleSgt") or svc.get("cronUtc") or "Continuous"
              ),
              "status": (
                  "PAUSED"
                  if is_paused
                  else ("ACTIVE" if svc.get("isRunning") else "STOPPED")
              ),
              "lastFired": (
                  (svc.get("recentLogs") or [""])[-1][:60]
                  if svc.get("recentLogs")
                  else "Scheduled"
              ),
              "canTogglePause": svc.get("sourceType") == "user-sidecar",
          })
        else:
          parts = (svc.get("id") or "").split("/", 1)
          sidecar_items.append({
              **svc,
              "plugin": parts[0],
              "sidecar": parts[1] if len(parts) > 1 else parts[0],
              "title": svc.get("displayName") or svc.get("id", ""),
              "type": svc.get("kind") or "ui-plugin",
              "status": "RUNNING" if svc.get("isRunning") else "STOPPED",
          })

      active_status = (
          active_conv_obj["status"]
          if active_conv_obj
          else (token_telemetry.get("lsStatus") or "IDLE")
      )
      subagents_list = chat_stream.get("subagents") or []
      active_subagents = max(
          active_conv_obj.get("subagentCount", 0) if active_conv_obj else 0,
          len(subagents_list),
      )

      self._send_json({
          "activeConversationId": active_conv_id,
          "languageServer": runtime.get("languageServer") or {},
          "conversations": {
              "activeId": active_conv_id,
              "hostActiveId": host_conv_id,
              "list": conversations,
          },
          "chat": {
              "conversationId": active_conv_id,
              "status": active_status,
              "totalSteps": max(
                  chat_stream.get("stepCount", 0),
                  active_conv_obj.get("stepCount", 0) if active_conv_obj else 0,
              ),
              "subagentCount": active_subagents,
              "subagents": subagents_list,
              "items": chat_stream.get("items") or [],
          },
          "tokens": {
              "activeSession": {
                  **token_telemetry,
                  "cacheHitPct": token_telemetry.get("cacheHitRatePct", 0.0),
                  "estCostUsd": token_telemetry.get("estimatedCostUsd", 0.0),
                  "turnCount": token_telemetry.get("llmCalls", 0),
                  "turns": token_telemetry.get("generations") or [],
              },
              "globalRecent": {
                  **global_tokens,
                  "sessionCount": global_tokens.get("sessionsCounted", 0),
                  "estCostUsd": global_tokens.get("estimatedCostUsd", 0.0),
              },
          },
          "automations": {
              "activeCount": sum(
                  1 for x in cron_items if x["status"] == "ACTIVE"
              ),
              "items": cron_items,
          },
          "sidecars": {
              "activeCount": sum(
                  1 for x in sidecar_items if x["status"] == "RUNNING"
              ),
              "items": sidecar_items,
          },
          "mcp": {
              "servers": [
                  {**m, "lazyCount": m.get("toolCount", 0)}
                  for m in (runtime.get("mcpServers") or [])
              ],
          },
          "memories": {
              "items": runtime.get("recentMemories") or [],
          },
      })
      return

    self._send_json({"error": "Not found"}, status=404)

  def do_POST(self):
    parsed = urllib.parse.urlparse(self.path)
    path = parsed.path

    try:
      length = int(self.headers.get("Content-Length", "0"))
      raw_body = self.rfile.read(length) if length > 0 else b"{}"
      data = json.loads(raw_body.decode("utf-8")) if raw_body else {}
    except Exception as e:
      self._send_json({"error": f"Invalid JSON: {e}"}, status=400)
      return

    # Unified prompt dispatch (/api/chat/send, /_sidecar/send-message, /_sidecar/new-conversation)
    if path in (
        "/api/chat/send",
        "/_sidecar/send-message",
        "/api/harness/send-message",
        "/_sidecar/new-conversation",
        "/api/harness/new-conversation",
    ):
      prompt = (data.get("prompt") or data.get("message") or "").strip()
      if not prompt:
        self._send_json({"ok": False, "error": "Missing prompt"}, status=400)
        return
      force_new = path in (
          "/_sidecar/new-conversation",
          "/api/harness/new-conversation",
      )
      conv_id = "" if force_new else (data.get("convId") or data.get("conversationId") or "").strip()
      if path in ("/_sidecar/send-message", "/api/harness/send-message") and not conv_id:
        conv_id = _resolve_request_conv_id(body=data)
      try:
        if conv_id:
          res = _send_message_direct(
              conv_id,
              prompt,
              title=data.get("title") or "",
              project_id=data.get("projectId"),
          )
        else:
          res = _start_conversation_direct(
              prompt,
              title=data.get("title") or "",
              model_tier=data.get("model") or "pro",
              project_id=data.get("projectId"),
          )
        self._send_json(res)
      except Exception as e:
        self._send_json({"ok": False, "error": str(e)}, status=500)
      return

    # Unified stop conversation (/api/chat/stop, /api/harness/cancel)
    if path in ("/api/chat/stop", "/api/harness/cancel"):
      conv_id = _resolve_request_conv_id(body=data)
      if not conv_id:
        self._send_json({"ok": False, "error": "Missing convId"}, status=400)
        return
      resp = _call_ls(
          "CancelCascadeInvocation", {"cascadeId": conv_id}, timeout=3.0
      )
      self._send_json({"ok": True, "response": resp})
      return

    # Pause / Resume an automation via restart_policy
    if path == "/api/automation/toggle":
      sidecar_id = (data.get("plugin") or data.get("sidecarId") or "").strip()
      if not sidecar_id or "/" in sidecar_id or ".." in sidecar_id:
        self._send_json({"ok": False, "error": "Invalid sidecarId"}, status=400)
        return
      sjson_path = os.path.join(
          CONFIG_DIR, "sidecars", sidecar_id, "sidecar.json"
      )
      if not os.path.isfile(sjson_path):
        self._send_json(
            {"ok": False, "error": f"Automation {sidecar_id} not found"},
            status=404,
        )
        return
      try:
        with open(sjson_path, "r", encoding="utf-8") as f:
          cfg = json.load(f)
        cur_policy = cfg.get("restart_policy") or "always"
        new_policy = "never" if cur_policy == "always" else "always"
        cfg["restart_policy"] = new_policy
        with open(sjson_path, "w", encoding="utf-8") as f:
          json.dump(cfg, f, indent=2)
          f.write("\n")
        _AUTOMATIONS_CACHE["ts"] = 0.0
        self._send_json(
            {"ok": True, "plugin": sidecar_id, "restartPolicy": new_policy}
        )
      except Exception as e:
        self._send_json({"ok": False, "error": str(e)}, status=500)
      return

    # Trigger an automation on-demand
    if path == "/api/automation/trigger":
      sidecar_id = (data.get("plugin") or data.get("sidecarId") or "").strip()
      payload, status = _trigger_automation_by_id(sidecar_id)
      self._send_json(payload, status=status)
      return

    if path == "/api/action":
      action = (data.get("action") or "").strip()
      conv_id = _resolve_request_conv_id(body=data)
      prompt = (data.get("prompt") or data.get("message") or "").strip()

      if action == "send_message":
        if not prompt or not conv_id:
          self._send_json(
              {"ok": False, "error": "Missing prompt or conversationId"},
              status=400,
          )
          return
        try:
          self._send_json(
              _send_message_direct(
                  conv_id,
                  prompt,
                  title=data.get("title") or "",
                  project_id=data.get("projectId"),
              )
          )
        except Exception as e:
          self._send_json({"ok": False, "error": str(e)}, status=500)
        return

      if action == "start_conversation":
        if not prompt:
          self._send_json({"ok": False, "error": "Missing prompt"}, status=400)
          return
        try:
          self._send_json(
              _start_conversation_direct(
                  prompt,
                  title=data.get("title") or "",
                  model_tier=data.get("model") or "pro",
                  project_id=data.get("projectId"),
              )
          )
        except Exception as e:
          self._send_json({"ok": False, "error": str(e)}, status=500)
        return

      if action == "stop_conversation":
        if not conv_id:
          self._send_json(
              {"ok": False, "error": "Missing conversationId"}, status=400
          )
          return
        resp = _call_ls(
            "CancelCascadeInvocation", {"cascadeId": conv_id}, timeout=3.0
        )
        self._send_json({"ok": True, "response": resp})
        return

      if action == "trigger_automation":
        sidecar_id = (data.get("sidecarId") or data.get("plugin") or "").strip()
        payload, status = _trigger_automation_by_id(sidecar_id)
        self._send_json(payload, status=status)
        return

      self._send_json(
          {"ok": False, "error": f"Unknown action: {action}"}, status=400
      )
      return

    if path == "/api/update":
      try:
        out = subprocess.check_output(
            ["git", "-C", TRACER_DIR, "pull", "--ff-only"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=15,
        ).strip()
        _UPDATE_CACHE["ts"] = 0.0
        self._send_json({"ok": True, "output": out})
      except Exception as e:
        self._send_json({"ok": False, "error": str(e)}, status=500)
      return

    if path == "/_sidecar/get-conversation-metadata":
      conv_id = _resolve_request_conv_id(body=data)
      if not conv_id:
        self._send_json({"error": "Missing conversationId"}, status=400)
        return
      resp = _call_ls(
          "GetConversationMetadata", {"conversationId": conv_id}, timeout=2.5
      )
      metadata = (resp or {}).get("metadata") if isinstance(resp, dict) else {}
      self._send_json(
          {"response": {"conversationMetadata": {"metadata": metadata or {}}}}
      )
      return

    self._send_json({"error": "Not found"}, status=404)


def main():
  port = int(
      os.environ.get("ANTIGRAVITY_SIDECAR_WEB_PORT")
      or os.environ.get("PORT")
      or "8765"
  )
  server = ThreadingHTTPServer(("0.0.0.0", port), HarnessRequestHandler)
  print(
      f"[jetski-harness] Custom Harness v2.9 listening on http://0.0.0.0:{port}",
      flush=True,
  )
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
  finally:
    server.server_close()


if __name__ == "__main__":
  main()
