#!/usr/bin/env python3
"""Custom Jetski Harness & Mission Control Sidecar Backend (v2.9)."""

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
TRACER_DIR = os.path.join(CONFIG_DIR, "plugins", "agent-tracer", "sidecars", "tracer")
PRELOAD_SDK_PATH = os.path.join(BASE_DIR, "preload.js")

SGT_TZ = timezone(timedelta(hours=8), name="SGT")
_UUID_RE = re.compile(r"^[a-fA-F0-9-]+$")
_CONV_DIR_RE = re.compile(r"^[a-fA-F0-9-]{20,}$")
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

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
        os._exit(0)
    except OSError:
      pass


threading.Thread(target=_watch_self, daemon=True).start()

_CACHE_LOCK = threading.Lock()
_MAX_CONV_CACHE = 64
_LS_CONN_CACHE = {"address": None, "csrf": None, "pid": None, "checked_at": 0.0}
_STEP_CACHE: OrderedDict[str, dict] = OrderedDict()
_FULL_STEP_CONTENT_CACHE: OrderedDict[tuple[str, int], dict] = OrderedDict()
_TOKEN_USAGE_CACHE: OrderedDict[str, tuple[int, float, dict]] = OrderedDict()
_CHAT_STREAM_CACHE: OrderedDict[str, tuple[int, int, dict]] = OrderedDict()
_STATIC_FILE_CACHE: dict[str, tuple[int, int, bytes]] = {}
_UPDATE_CACHE = {"data": None, "ts": 0.0}
_LS_TRAJECTORIES_CACHE = {"ts": 0.0, "summaries": {}}
_BRAIN_ENTRIES_CACHE = {"ts": 0.0, "ids": set()}
_AUTOMATIONS_CACHE = {"ts": 0.0, "data": []}
_RUNTIME_STATUS_CACHE = {"ts": 0.0, "data": {}}
_MODEL_LABEL_CACHE = {"ts": 0.0, "map": {}}


def _lru_get(cache: OrderedDict, key):
  with _CACHE_LOCK:
    if key in cache:
      cache.move_to_end(key)
      return cache[key]
    return None


def _lru_set(cache: OrderedDict, key, value, max_size: int = _MAX_CONV_CACHE):
  with _CACHE_LOCK:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_size:
      cache.popitem(last=False)


def _read_cached_file_bytes(fpath: str) -> bytes | None:
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
  now = time.time()
  c = _LS_CONN_CACHE
  if not force and c["address"] and c["csrf"] and (now - c["checked_at"] < 30.0):
    return c["address"], c["csrf"], c["pid"]

  env_addr = os.environ.get("ANTIGRAVITY_LS_ADDRESS")
  env_csrf = os.environ.get("ANTIGRAVITY_CSRF_TOKEN")
  if env_addr and env_csrf and not force:
    c.update({"address": env_addr, "csrf": env_csrf, "pid": None, "checked_at": now})
    return env_addr, env_csrf, None

  try:
    res = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True, text=True, timeout=3)
    for line in res.stdout.splitlines():
      if "language_server" not in line or "--csrf_token" not in line:
        continue
      parts = line.strip().split(None, 1)
      csrf_m = re.search(r"--csrf_token[=\s]+([a-fA-F0-9-]+)", parts[1] if len(parts) > 1 else "")
      if len(parts) < 2 or not csrf_m:
        continue
      pid, csrf = parts[0], csrf_m.group(1)
      ss_res = subprocess.run(["ss", "-tlpn"], capture_output=True, text=True, timeout=3)
      ports = [
          int(pm.group(1))
          for sline in ss_res.stdout.splitlines()
          if f"pid={pid}," in sline and (pm := re.search(r"127\.0\.0\.1:(\d+)", sline))
      ]
      for pt in sorted(ports):
        addr = f"127.0.0.1:{pt}"
        try:
          url = f"http://{addr}/exa.language_server_pb.LanguageServerService/GetMcpServerStates"
          headers = {"Content-Type": "application/json", "x-codeium-csrf-token": csrf}
          req = urllib.request.Request(url, data=b"{}", headers=headers, method="POST")
          with _NO_PROXY_OPENER.open(req, timeout=0.8) as resp:
            if resp.status == 200:
              c.update({"address": addr, "csrf": csrf, "pid": int(pid), "checked_at": now})
              return addr, csrf, int(pid)
        except Exception:
          continue
  except Exception:
    pass
  return env_addr, env_csrf, None


def _call_ls(method: str, payload: dict | None = None, timeout: float = 1.5):
  addr, csrf, _ = _discover_language_server(force=False)
  if not addr or not csrf:
    return None
  body = json.dumps(payload or {}).encode("utf-8")
  for attempt in range(2):
    try:
      url = f"http://{addr}/exa.language_server_pb.LanguageServerService/{method}"
      headers = {"Content-Type": "application/json", "x-codeium-csrf-token": csrf}
      req = urllib.request.Request(url, data=body, headers=headers, method="POST")
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
  if isinstance(val, (int, float)):
    return float(val)
  if isinstance(val, str) and val.endswith("s"):
    try:
      return float(val[:-1])
    except ValueError:
      pass
  return 0.0


_MODEL_PRICING_TIERS = {
    "flash_lite": {"inputPer1M": 0.10, "cachedPer1M": 0.025, "outputPer1M": 0.40},
    "flash": {"inputPer1M": 0.30, "cachedPer1M": 0.075, "outputPer1M": 2.50},
    "claude_opus": {"inputPer1M": 15.00, "cachedPer1M": 1.50, "outputPer1M": 75.00},
    "claude_sonnet": {"inputPer1M": 3.00, "cachedPer1M": 0.30, "outputPer1M": 15.00},
    "pro": {"inputPer1M": 1.25, "cachedPer1M": 0.3125, "outputPer1M": 10.00},
}


def _get_pricing_rates(model_label: str = "", raw_model: str = "") -> dict:
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


def _estimate_cost_usd(uncached_in: int, out_tok: int, think_tok: int, cache_tok: int, label: str = "", raw: str = "") -> float:
  r = _get_pricing_rates(label, raw)
  cost = (
      (max(0, uncached_in) * r["inputPer1M"])
      + (max(0, cache_tok) * r["cachedPer1M"])
      + (max(0, out_tok + think_tok) * r["outputPer1M"])
  ) / 1_000_000.0
  return round(cost, 4)


def _get_transcript_path(conv_id: str, full: bool = False) -> str | None:
  if not conv_id or not _UUID_RE.match(conv_id):
    return None
  fname = "transcript_full.jsonl" if full else "transcript.jsonl"
  for app_name in ("jetski", "antigravity"):
    p = os.path.join(HOME_DIR, ".gemini", app_name, "brain", conv_id, ".system_generated", "logs", fname)
    if os.path.isfile(p):
      return p
  return os.path.join(BRAIN_DIR, conv_id, ".system_generated", "logs", fname)


def _compute_step_status(last_type: str, last_status: str, turn_done: bool, mtime: float) -> str:
  if last_status in ("RUNNING", "IN_PROGRESS", "PENDING"):
    return "RUNNING"
  if not turn_done and mtime and (time.time() - mtime < 15.0) and last_type in ("USER_INPUT", "PLANNER_RESPONSE", "GENERIC"):
    return "RUNNING"
  return "ERROR" if last_status == "ERROR" else "IDLE"


def _empty_summary() -> dict:
  return {
      "exists": False, "stepCount": 0, "toolCallCount": 0, "subagentCount": 0,
      "title": "", "status": "IDLE", "turnCompleted": False, "lastStepType": "",
      "lastStepStatus": "", "lastAction": "", "updatedAt": "", "createdAt": "",
      "charCount": 0, "mtime": 0.0, "_hasTools": False, "_hasContent": False
  }


def _get_cached_transcript_bundle(conv_id: str) -> dict:
  """Incrementally tails transcript.jsonl by byte offset and caches parsed steps + summary."""
  tpath = _get_transcript_path(conv_id, full=False)
  if not tpath or not os.path.isfile(tpath):
    return {"exists": False, "mtime": 0.0, "mtime_ns": 0, "size": 0, "steps": [], "summary": _empty_summary()}

  try:
    st = os.stat(tpath)
    mtime, mtime_ns, fsize = st.st_mtime, st.st_mtime_ns, st.st_size
  except OSError:
    mtime, mtime_ns, fsize = 0.0, 0, 0

  cached = _lru_get(_STEP_CACHE, conv_id)
  if cached and cached["mtime_ns"] == mtime_ns and cached["size"] == fsize:
    s = cached["summary"]
    if mtime and (time.time() - mtime < 30.0):
      s = {**s, "status": _compute_step_status(s["lastStepType"], s["lastStepStatus"], s["turnCompleted"], mtime)}
    return {**cached, "summary": s}

  if cached and 0 < cached["offset"] <= fsize:
    offset, steps, s = cached["offset"], list(cached["steps"]), dict(cached["summary"])
  else:
    offset, steps, s = 0, [], {**_empty_summary(), "exists": True}

  try:
    with open(tpath, "rb") as f:
      if offset > 0:
        f.seek(offset)
      raw_chunk = f.read()
  except OSError:
    raw_chunk = b""

  last_nl = raw_chunk.rfind(b"\n") if raw_chunk else -1
  new_offset = (offset + last_nl + 1) if last_nl != -1 else offset
  if last_nl != -1:
    for raw_line in raw_chunk[: last_nl + 1].decode("utf-8", errors="replace").splitlines():
      line = raw_line.strip()
      if not line:
        continue
      s["charCount"] += len(line)
      try:
        obj = json.loads(line)
      except json.JSONDecodeError:
        continue
      steps.append(obj)
      s["stepCount"] += 1
      stype, sstatus, screated = obj.get("type", ""), obj.get("status", ""), obj.get("created_at", "")
      if not s["createdAt"] and screated:
        s["createdAt"] = screated
      if screated:
        s["updatedAt"] = screated
      s["lastStepType"], s["lastStepStatus"] = stype, sstatus

      if stype == "USER_INPUT" and not s["title"]:
        txt = re.sub(r"</?USER_REQUEST>", "", (obj.get("content") or "").strip()).strip()
        if txt:
          s["title"] = txt.splitlines()[0][:90]

      tcalls = obj.get("tool_calls") or []
      if stype == "PLANNER_RESPONSE":
        s["_hasTools"] = bool(isinstance(tcalls, list) and tcalls)
        s["_hasContent"] = bool((obj.get("content") or "").strip())

      if isinstance(tcalls, list) and tcalls:
        s["toolCallCount"] += len(tcalls)
        for tc in tcalls:
          tname = tc.get("name", "")
          if tname == "invoke_subagent":
            s["subagentCount"] += 1
          args = tc.get("arguments") or tc.get("args") or {}
          if isinstance(args, dict):
            act = args.get("toolAction") or args.get("toolSummary") or tname
            if isinstance(act, str) and act.strip(' "'):
              s["lastAction"] = act.strip(' "')

  if not s["updatedAt"] and mtime:
    s["updatedAt"] = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
  s["mtime"] = mtime
  s["turnCompleted"] = (
      s["lastStepType"] == "PLANNER_RESPONSE"
      and s["lastStepStatus"] == "DONE"
      and s["_hasContent"]
      and not s["_hasTools"]
  )
  s["status"] = _compute_step_status(s["lastStepType"], s["lastStepStatus"], s["turnCompleted"], mtime)

  bundle = {"exists": True, "mtime": mtime, "mtime_ns": mtime_ns, "size": fsize, "offset": new_offset, "steps": steps, "summary": s}
  _lru_set(_STEP_CACHE, conv_id, bundle)
  return bundle


def _summarize_transcript_fast(conv_id: str) -> dict:
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
  if not raw_model:
    return "Gemini Next"
  now = time.time()
  if not _MODEL_LABEL_CACHE["map"] or (now - _MODEL_LABEL_CACHE["ts"] > 300.0):
    label_map = dict(_KNOWN_PLACEHOLDER_LABELS)
    try:
      avail = _call_ls("GetAvailableModels", {}, timeout=1.5) or {}
      for info in ((avail.get("response") or {}).get("models") or {}).values():
        if isinstance(info, dict) and info.get("model") and info.get("displayName"):
          label_map[info["model"]] = info["displayName"]
      cfg_data = _call_ls("GetCascadeModelConfigData", {}, timeout=1.5) or {}
      for cfg in cfg_data.get("clientModelConfigs") or []:
        if isinstance(cfg, dict):
          m_enum, lbl = (cfg.get("modelOrAlias") or {}).get("model"), cfg.get("label")
          if m_enum and lbl:
            label_map[m_enum] = lbl
    except Exception:
      pass
    _MODEL_LABEL_CACHE.update({"map": label_map, "ts": now})

  if raw_model in _MODEL_LABEL_CACHE["map"]:
    return _MODEL_LABEL_CACHE["map"][raw_model]
  if raw_model.startswith("MODEL_PLACEHOLDER_"):
    return "Gemini Next"
  return raw_model.replace("MODEL_GOOGLE_", "").replace("MODEL_", "").replace("_", " ").title()


def _make_token_result(
    conv_id: str, is_est: bool, ls_status: str, raw_m: str, provider: str,
    calls: int, uncached_in: int, out_tok: int, think_tok: int, cache_tok: int,
    ctx_win: int, avg_ttft: float, stream_sec: float, gens: list
) -> dict:
  prompt_tok = uncached_in + cache_tok
  total_tok = prompt_tok + out_tok + think_tok
  hit_pct = round((cache_tok / prompt_tok) * 100.0, 1) if prompt_tok > 0 else 0.0
  label = _get_model_display_name(raw_m)
  return {
      "conversationId": conv_id, "isEstimated": is_est, "lsStatus": ls_status,
      "model": label, "rawModel": raw_m, "pricingRates": _get_pricing_rates(label, raw_m),
      "apiProvider": provider, "llmCalls": calls, "inputTokens": prompt_tok,
      "promptTokens": prompt_tok, "uncachedInputTokens": uncached_in,
      "outputTokens": out_tok, "thinkingTokens": think_tok, "cacheReadTokens": cache_tok,
      "cachedTokens": cache_tok, "totalTokens": total_tok, "cacheHitRatePct": hit_pct,
      "contextWindowTokens": ctx_win,
      "estimatedCostUsd": _estimate_cost_usd(uncached_in, out_tok, think_tok, cache_tok, label, raw_m),
      "avgTtftSeconds": avg_ttft, "totalStreamingSeconds": stream_sec,
      "lastTurn": gens[-1] if gens else {}, "generations": gens, "perTurn": gens
  }


def _get_token_telemetry(conv_id: str, include_generations: bool = True, allow_rpc: bool = True) -> dict:
  bundle = _get_cached_transcript_bundle(conv_id)
  tmeta, mtime_ns = bundle["summary"], bundle["mtime_ns"]
  mtime, now = tmeta.get("mtime", 0.0), time.time()

  cached = _lru_get(_TOKEN_USAGE_CACHE, conv_id)
  ttl = 12.0 if (now - mtime < 30.0) else 300.0
  if cached:
    c_mtime_ns, c_ts, c_res = cached
    needs_upgrade = allow_rpc and (c_res.get("isEstimated") or (include_generations and not c_res.get("generations")))
    if not needs_upgrade and ((c_mtime_ns == mtime_ns and (now - c_ts < ttl)) or not allow_rpc):
      return c_res

  traj_resp = _call_ls("GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.0) if allow_rpc else None
  traj = traj_resp.get("trajectory") if isinstance(traj_resp, dict) and isinstance(traj_resp.get("trajectory"), dict) else {}
  gen_meta, ls_status = traj.get("generatorMetadata") or [], traj.get("status", "")

  if gen_meta:
    u_in = out_t = th_t = c_t = ttft_cnt = 0
    ttft_sum = stream_sum = 0.0
    m_name = prov = ""
    gens = []
    for idx, gm in enumerate(gen_meta):
      cm = gm.get("chatModel") or {}
      usage = cm.get("usage") or {}
      cur_m = cm.get("model") or usage.get("model") or ""
      m_name = cur_m or m_name
      prov = (usage.get("apiProvider") or "").replace("API_PROVIDER_", "") or prov
      i_tok, o_tok = int(usage.get("inputTokens") or 0), int(usage.get("outputTokens") or 0)
      th_tok, ca_tok = int(usage.get("thinkingOutputTokens") or 0), int(usage.get("cacheReadTokens") or 0)
      ttft, sdur = _parse_duration_seconds(cm.get("timeToFirstToken")), _parse_duration_seconds(cm.get("streamingDuration"))
      s_idxs = gm.get("stepIndices") or []
      u_in += i_tok
      out_t += o_tok
      th_t += th_tok
      c_t += ca_tok
      if ttft > 0:
        ttft_sum += ttft
        ttft_cnt += 1
      stream_sum += sdur
      gens.append({
          "turn": idx + 1, "stepIndex": s_idxs[0] if s_idxs else idx,
          "inputTokens": i_tok + ca_tok, "promptTokens": i_tok + ca_tok,
          "uncachedInputTokens": i_tok, "outputTokens": o_tok, "thinkingTokens": th_tok,
          "cacheReadTokens": ca_tok, "cachedTokens": ca_tok,
          "ttftSeconds": round(ttft, 2), "streamingSeconds": round(sdur, 2),
          "model": _get_model_display_name(cur_m), "rawModel": cur_m
      })
    ctx_win = (gens[-1]["inputTokens"] + gens[-1]["outputTokens"]) if gens else 0
    res = _make_token_result(
        conv_id, False, ls_status, m_name or "MODEL_PLACEHOLDER_M260", prov or "INTERNAL",
        len(gens), u_in, out_t, th_t, c_t, ctx_win,
        round(ttft_sum / ttft_cnt, 2) if ttft_cnt else 0.0, round(stream_sum, 1),
        gens[-24:] if include_generations else []
    )
  else:
    est_out = max(1, tmeta.get("charCount", 0) // 4)
    est_in = est_out * max(1, min(tmeta.get("stepCount", 1), 12))
    est_cache = int(est_in * 0.78)
    res = _make_token_result(
        conv_id, True, ls_status or "ARCHIVED", "MODEL_PLACEHOLDER_M260", "GOOGLE_GEMINI_INTERNAL",
        max(1, tmeta.get("stepCount", 1) // 2), max(0, est_in - est_cache), est_out,
        int(est_out * 0.35), est_cache, min(est_in, 128000), 0.0, 0.0, []
    )

  _lru_set(_TOKEN_USAGE_CACHE, conv_id, (mtime_ns, now, res))
  return res


def _list_conversations(active_conv_id: str) -> tuple[list[dict], dict]:
  now = time.time()
  if _LS_TRAJECTORIES_CACHE["summaries"] and (now - _LS_TRAJECTORIES_CACHE["ts"] < 6.0):
    summaries = _LS_TRAJECTORIES_CACHE["summaries"]
  else:
    ls_all = _call_ls("GetAllCascadeTrajectories", {}, timeout=1.8) or {}
    summaries = ls_all.get("trajectorySummaries") or {}
    _LS_TRAJECTORIES_CACHE.update({"ts": now, "summaries": summaries})

  conv_ids = set(summaries.keys())
  if _BRAIN_ENTRIES_CACHE["ids"] and (now - _BRAIN_ENTRIES_CACHE["ts"] < 12.0):
    conv_ids.update(_BRAIN_ENTRIES_CACHE["ids"])
  elif os.path.isdir(BRAIN_DIR):
    try:
      brain_ids = {
          e for e in os.listdir(BRAIN_DIR)
          if _CONV_DIR_RE.match(e) and (tp := _get_transcript_path(e, False)) and os.path.isfile(tp)
      }
    except OSError:
      brain_ids = set()
    _BRAIN_ENTRIES_CACHE.update({"ts": now, "ids": brain_ids})
    conv_ids.update(brain_ids)

  items = []
  for cid in conv_ids:
    ls_info = summaries.get(cid) or {}
    tmeta = _summarize_transcript_fast(cid)
    ls_st = (ls_info.get("status") or "").replace("CASCADE_RUN_STATUS_", "")
    if ls_st in ("RUNNING", "IN_PROGRESS", "ACTIVE"):
      status = "RUNNING"
    elif ls_st in ("IDLE", "COMPLETED", "DONE"):
      status = "IDLE" if tmeta["status"] != "RUNNING" else "RUNNING"
    else:
      status = tmeta["status"]

    if cid == active_conv_id and status == "IDLE" and not tmeta.get("turnCompleted") and (now - tmeta.get("mtime", 0) < 12):
      status = "RUNNING"

    step_cnt = max(int(ls_info.get("stepCount") or 0), tmeta.get("stepCount", 0))
    ws = ls_info.get("workspaces") or []
    uri = (ws[0].get("workspaceFolderAbsoluteUri") or "") if (ws and isinstance(ws[0], dict)) else ""
    ws_label = (uri.replace("file://", "").rstrip("/").split("/")[-1] or uri) if uri else "No Workspace (Scratch)"

    items.append({
        "id": cid, "conversationId": cid,
        "title": ls_info.get("summary") or tmeta.get("title") or f"Session {cid[:8]}",
        "status": status, "isRunning": status == "RUNNING",
        "stepCount": step_cnt, "steps": step_cnt,
        "toolCallCount": tmeta.get("toolCallCount", 0),
        "subagentCount": tmeta.get("subagentCount", 0),
        "lastAction": tmeta.get("lastAction") or tmeta.get("lastStepType") or "Idle",
        "updatedAt": ls_info.get("lastModifiedTime") or tmeta.get("updatedAt") or "",
        "createdAt": ls_info.get("createdTime") or tmeta.get("createdAt") or "",
        "workspace": ws_label, "isCurrent": cid == active_conv_id, "mtime": tmeta.get("mtime", 0.0)
    })

  items.sort(key=lambda x: (x["updatedAt"] or "", x["mtime"], x["id"]), reverse=True)
  top_items = items[:18]
  g_in = g_out = g_think = g_cache = g_calls = 0
  g_cost = 0.0

  for idx, item in enumerate(top_items):
    is_act = item["id"] == active_conv_id
    if idx < 6 or is_act:
      tok = _get_token_telemetry(item["id"], include_generations=is_act, allow_rpc=is_act)
      item.update({
          "tokens": tok, "totalTokens": tok.get("totalTokens", 0),
          "estimatedCostUsd": tok.get("estimatedCostUsd", 0.0), "cacheHitRatePct": tok.get("cacheHitRatePct", 0.0)
      })
      if idx < 6:
        g_in += tok.get("inputTokens", 0)
        g_out += tok.get("outputTokens", 0)
        g_think += tok.get("thinkingTokens", 0)
        g_cache += tok.get("cacheReadTokens", 0)
        g_cost += tok.get("estimatedCostUsd", 0.0)
        g_calls += tok.get("llmCalls", 0)

  global_summary = {
      "sessionsCounted": min(len(top_items), 6), "totalConversations": len(items),
      "runningCount": sum(1 for x in items if x["status"] == "RUNNING"),
      "inputTokens": g_in, "promptTokens": g_in, "outputTokens": g_out,
      "thinkingTokens": g_think, "cacheReadTokens": g_cache, "cachedTokens": g_cache,
      "totalTokens": g_in + g_out + g_think,
      "cacheHitRatePct": round((g_cache / g_in) * 100.0, 1) if g_in > 0 else 0.0,
      "estimatedCostUsd": round(g_cost, 4), "llmCalls": g_calls
  }
  return top_items, global_summary


def _format_uptime(seconds: int) -> str:
  if seconds <= 0:
    return "0s"
  d, rem = divmod(seconds, 86400)
  h, rem = divmod(rem, 3600)
  m, s = divmod(rem, 60)
  return f"{d}d {h}h" if d else (f"{h}h {m}m" if h else (f"{m}m {s}s" if m else f"{s}s"))


def _cron_utc_to_sgt_label(cron_expr: str) -> str:
  parts = cron_expr.strip().split()
  if len(parts) != 5:
    return cron_expr
  minute, hour, _, _, dow = parts
  dow_lbl = {"*": "Daily", "1-5": "Weekdays (Mon–Fri)", "0,6": "Weekends"}.get(dow, f"DOW {dow}")
  if hour.isdigit() and minute.isdigit():
    return f"{dow_lbl} at {(int(hour) + 8) % 24:02d}:{int(minute):02d} SGT ({int(hour):02d}:{int(minute):02d} UTC)"
  if hour == "*" and minute.isdigit():
    return f"Hourly at :{int(minute):02d} SGT/UTC ({dow_lbl})"
  if hour == "22-23,0-11" and minute.isdigit():
    return f"Hourly 06:{int(minute):02d}–19:{int(minute):02d} SGT ({dow_lbl})"
  return f"{cron_expr} UTC (SGT = UTC+8)"


_IGNORED_STATE_JSON_FILES = frozenset({"sidecar.json", "plugin.json", "package.json", "package-lock.json", "tsconfig.json"})


def _inspect_sidecar_state_badge(sdir: str) -> str:
  try:
    for fn in sorted(os.listdir(sdir)):
      if fn.endswith(".json") and fn not in _IGNORED_STATE_JSON_FILES and os.path.isfile(os.path.join(sdir, fn)):
        with open(os.path.join(sdir, fn), "r", encoding="utf-8") as cf:
          cdata = json.load(cf)
        if isinstance(cdata, (list, dict)):
          return f"{len(cdata)} items tracked"
  except Exception:
    pass
  return ""


def _list_automations_and_sidecars(force: bool = False) -> list[dict]:
  now = time.time()
  if not force and _AUTOMATIONS_CACHE["data"] and (now - _AUTOMATIONS_CACHE["ts"] < 30.0):
    return _AUTOMATIONS_CACHE["data"]

  procs = []
  try:
    res = subprocess.run(["ps", "-eo", "pid,etimes,args"], capture_output=True, text=True, timeout=3)
    for line in res.stdout.splitlines()[1:]:
      parts = line.strip().split(None, 2)
      if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
        pid_int = int(parts[0])
        try:
          cwd = os.path.realpath(f"/proc/{pid_int}/cwd")
        except OSError:
          cwd = ""
        procs.append({"pid": pid_int, "etimes": int(parts[1]), "cmd": parts[2], "cwd": cwd})
  except Exception:
    pass

  manifests = []
  loose_root = os.path.join(CONFIG_DIR, "sidecars")
  if os.path.isdir(loose_root):
    for name in sorted(os.listdir(loose_root)):
      sj = os.path.join(loose_root, name, "sidecar.json")
      if os.path.isfile(sj):
        manifests.append((name, "user-sidecar", os.path.join(loose_root, name), sj))

  for root_dir, src_type in ((os.path.join(CONFIG_DIR, "plugins"), "ui-plugin"), (os.path.join(JETSKI_DIR, "builtin", "plugins"), "builtin-sidecar")):
    if not os.path.isdir(root_dir):
      continue
    for pname in sorted(os.listdir(root_dir)):
      pdir = os.path.join(root_dir, pname)
      pjson, decl = os.path.join(pdir, "plugin.json"), pname
      if src_type == "ui-plugin" and os.path.isfile(pjson):
        try:
          with open(pjson, "r", encoding="utf-8") as pf:
            decl = json.load(pf).get("name") or pname
        except Exception:
          pass
      sroot = os.path.join(pdir, "sidecars")
      if os.path.isdir(sroot):
        for sname in sorted(os.listdir(sroot)):
          sj = os.path.join(sroot, sname, "sidecar.json")
          if os.path.isfile(sj):
            manifests.append((f"{decl}/{sname}", src_type, os.path.join(sroot, sname), sj))

  results = []
  for sid, source_type, sdir, sjson_path in manifests:
    try:
      with open(sjson_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    except Exception:
      continue

    builtin, cmd_name, args = cfg.get("builtin", ""), cfg.get("command", ""), cfg.get("args") or []
    has_web_ui = bool(cfg.get("has_web_ui"))
    display_name = cfg.get("display_name") or (cfg.get("ui_config") or {}).get("title") or sid
    restart_policy = cfg.get("restart_policy") or "never"
    kind = "ui-plugin" if has_web_ui else ("cron-automation" if builtin == "schedule" else "daemon")
    cron_utc = schedule_sgt = target_summary = ""

    if builtin == "schedule" and args:
      cron_utc = args[0]
      schedule_sgt = _cron_utc_to_sgt_label(cron_utc)
      if len(args) >= 3 and args[1] == "agentapi":
        sub = args[2]
        target_summary = f"agentapi send-message → {args[3][:8]}…" if (sub == "send-message" and len(args) >= 4) else ("agentapi new-conversation (headless)" if sub == "new-conversation" else f"agentapi {sub}")
      else:
        target_summary = " ".join(args[1:3])
    else:
      target_summary = f"{cmd_name} {' '.join(str(a) for a in args[:2])}".strip()
      if not has_web_ui and cmd_name.startswith("python"):
        cron_utc, schedule_sgt = "Continuous loop", "Continuous Daemon Loop"

    real_sdir = os.path.realpath(sdir)
    matched = None
    for p in procs:
      pcmd, pcwd = p["cmd"], p.get("cwd", "")
      if (
          (builtin == "schedule" and cron_utc and "multicall schedule" in pcmd and cron_utc in pcmd)
          or (pcwd and pcwd == real_sdir)
          or (sdir in pcmd or real_sdir in pcmd)
          or (len(args) == 1 and args[0] != "server.py" and args[0] in pcmd)
          or (sid == "jetski-harness/console" and p["pid"] == os.getpid())
      ):
        matched = p
        break

    extra_badge = _inspect_sidecar_state_badge(sdir)
    prompt_preview = ""
    if builtin == "schedule" and len(args) >= 3 and args[1] == "agentapi":
      raw_p = str(args[-1]).strip() if len(args) >= 4 else ""
      prompt_preview = raw_p[:360] + ("…" if len(raw_p) > 360 else "")
      mechanism_desc = f"Scheduled via Jetski's builtin 'schedule' runner ({cron_utc} UTC) invoking `{target_summary}`."
      safety_desc = f"Controlled by SidecarManager (`restart_policy: {restart_policy}`). Can be paused/resumed or triggered on-demand."
    elif has_web_ui:
      mechanism_desc = f"Always-on HTTP UI sidecar (`{target_summary}`, `has_web_ui: true`) mounted into the Jetski auxiliary pane."
      safety_desc = "Read-only local telemetry inspection and authenticated JSON-RPC bridge."
    else:
      mechanism_desc = f"Background daemon process (`{target_summary}`) managed by SidecarManager with `restart_policy: {restart_policy}`."
      safety_desc = f"Deduplicates state locally{f' ({extra_badge})' if extra_badge else ''} and operates in read-only monitoring mode."

    log_lines = []
    for cid in (sid, sid.replace("/", "_"), sid.split("/")[-1]):
      lfile = os.path.join(JETSKI_DIR, "sidecar_data", cid, "logs", "sidecar.log")
      if os.path.isfile(lfile):
        try:
          with open(lfile, "r", encoding="utf-8", errors="replace") as lf:
            log_lines = [ln.strip() for ln in lf if ln.strip()][-6:]
          break
        except Exception:
          pass

    results.append({
        "id": sid, "displayName": display_name, "description": cfg.get("description") or "",
        "kind": kind, "sourceType": source_type, "restartPolicy": restart_policy,
        "hasWebUi": has_web_ui, "cronUtc": cron_utc,
        "scheduleSgt": schedule_sgt or ("Always-On Web UI Sidecar" if has_web_ui else "Continuous Daemon"),
        "targetSummary": target_summary, "promptPreview": prompt_preview,
        "mechanismDesc": mechanism_desc, "safetyDesc": safety_desc,
        "isRunning": matched is not None, "pid": matched["pid"] if matched else None,
        "uptimeSeconds": matched["etimes"] if matched else 0,
        "uptimeFormatted": _format_uptime(matched["etimes"]) if matched else "Stopped",
        "extraBadge": extra_badge, "recentLogs": log_lines,
        "canTriggerNow": builtin == "schedule" and len(args) >= 3 and args[1] == "agentapi"
    })

  kind_order = {"cron-automation": 0, "daemon": 1, "ui-plugin": 2}
  results.sort(key=lambda r: (0 if r["isRunning"] else 1, kind_order.get(r["kind"], 3), r["id"]))
  _AUTOMATIONS_CACHE.update({"ts": now, "data": results})
  return results


def _get_runtime_and_mcp_status(force: bool = False) -> dict:
  now = time.time()
  if not force and _RUNTIME_STATUS_CACHE["data"] and (now - _RUNTIME_STATUS_CACHE["ts"] < 30.0):
    return _RUNTIME_STATUS_CACHE["data"]

  oauth_path = os.path.join(JETSKI_DIR, "mcp_oauth_tokens.json")
  oauth_map = {}
  if os.path.isfile(oauth_path):
    try:
      with open(oauth_path, "r", encoding="utf-8") as f:
        raw_o = json.load(f)
      if isinstance(raw_o, dict):
        oauth_map = raw_o.get("tokens") if isinstance(raw_o.get("tokens"), dict) else raw_o
    except Exception:
      pass

  mcp_root = os.path.join(JETSKI_DIR, "mcp")
  mcp_servers = []
  if os.path.isdir(mcp_root):
    for sname in sorted(os.listdir(mcp_root)):
      sdir = os.path.join(mcp_root, sname)
      if not os.path.isdir(sdir):
        continue
      tools = [fn[:-5] for fn in sorted(os.listdir(sdir)) if fn.endswith(".json") and fn != "mcp_config.json"]
      mcp_servers.append({
          "name": sname, "shortName": sname.split("_google_")[-1] if "_google_" in sname else sname,
          "displayName": sname.replace("_google_", " · ").replace("_", " ").title(),
          "toolCount": len(tools), "sampleTools": tools[:5],
          "status": "READY" if tools else "EMPTY", "oauthAuthenticated": bool(oauth_map)
      })

  memory_files = []
  if os.path.isdir(MEMORY_DIR):
    for root, _, files in os.walk(MEMORY_DIR):
      for fn in files:
        if fn.endswith(".md"):
          full_p = os.path.join(root, fn)
          try:
            mt = os.path.getmtime(full_p)
          except OSError:
            mt = 0.0
          memory_files.append({
              "path": os.path.relpath(full_p, MEMORY_DIR),
              "updatedAt": datetime.fromtimestamp(mt, tz=SGT_TZ).strftime("%Y-%m-%d %H:%M SGT") if mt else "",
              "mtime": mt
          })
  memory_files.sort(key=lambda x: x["mtime"], reverse=True)

  ls_addr, _, ls_pid = _discover_language_server(force=False)
  now_utc = datetime.now(timezone.utc)
  res = {
      "mcpServers": mcp_servers, "mcpTotalTools": sum(s["toolCount"] for s in mcp_servers),
      "oauthConfigured": bool(oauth_map), "oauthGrantsCount": len(oauth_map),
      "memoryMounted": os.path.isdir(MEMORY_DIR), "memoryFileCount": len(memory_files),
      "recentMemories": memory_files[:8],
      "languageServer": {"online": bool(ls_addr), "address": ls_addr or "disconnected", "pid": ls_pid},
      "host": {
          "hostname": os.uname().nodename, "user": os.environ.get("USER", "user"),
          "timeUtc": now_utc.strftime("%H:%M:%S UTC"),
          "timeSgt": now_utc.astimezone(SGT_TZ).strftime("%Y-%m-%d %H:%M:%S SGT"),
          "sidecarPid": os.getpid(), "sidecarPort": int(os.environ.get("ANTIGRAVITY_SIDECAR_WEB_PORT", 0))
      }
  }
  _RUNTIME_STATUS_CACHE.update({"ts": now, "data": res})
  return res


def _synthesize_steps_from_ls_trajectory(conv_id: str) -> list[dict]:
  if not conv_id:
    return []
  resp = _call_ls("GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.5)
  if not resp or not isinstance(resp.get("trajectory"), dict):
    return []
  out = []
  for idx, s in enumerate(resp["trajectory"].get("steps") or []):
    if not isinstance(s, dict):
      continue
    stype = s.get("type") or ""
    meta = s.get("metadata") or {}
    created = meta.get("createdAt") or meta.get("created_at") or ""
    status = (s.get("status") or "DONE").replace("CORTEX_STEP_STATUS_", "")
    if stype == "CORTEX_STEP_TYPE_USER_INPUT":
      ui = s.get("userInput") or {}
      text = ui.get("userResponse") or "".join(c.get("text", "") for c in (ui.get("items") or []) if isinstance(c, dict))
      if text:
        out.append({"step_index": idx, "type": "USER_INPUT", "status": "DONE", "created_at": created, "content": text})
    elif stype == "CORTEX_STEP_TYPE_PLANNER_RESPONSE":
      pr = s.get("plannerResponse") or {}
      tcalls = []
      for raw_tc in pr.get("toolCalls") or []:
        tc = raw_tc.get("toolCall", raw_tc) if isinstance(raw_tc, dict) else {}
        args_raw = tc.get("argumentsJson") or tc.get("arguments") or {}
        if isinstance(args_raw, str):
          try:
            args_raw = json.loads(args_raw)
          except Exception:
            args_raw = {"raw": args_raw}
        tcalls.append({"id": tc.get("id") or f"tc_{idx}_{len(tcalls)}", "name": tc.get("name") or "tool", "args": args_raw, "arguments": args_raw})
      out.append({
          "step_index": idx, "type": "PLANNER_RESPONSE",
          "status": "DONE" if status in ("DONE", "COMPLETED") else status,
          "created_at": created, "content": pr.get("modifiedResponse") or pr.get("response") or "",
          "thinking": pr.get("thinking") or "", "tool_calls": tcalls
      })
  return out


def _read_transcript_steps(conv_id: str, since: int = -1) -> list[dict]:
  steps = _get_cached_transcript_bundle(conv_id)["steps"]
  if steps:
    return steps if since < 0 else [s for s in steps if s.get("step_index", -1) > since]
  return [s for s in _synthesize_steps_from_ls_trajectory(conv_id) if s.get("step_index", -1) > since]


def _read_step_full(conv_id: str, step_idx: int) -> dict | None:
  cache_key = (conv_id, step_idx)
  if (cached := _lru_get(_FULL_STEP_CONTENT_CACHE, cache_key)) is not None:
    return cached
  for full_flag in (True, False):
    tpath = _get_transcript_path(conv_id, full=full_flag)
    if not tpath or not os.path.isfile(tpath):
      continue
    try:
      with open(tpath, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
          if not (line := raw.strip()):
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
  out = {}
  for cid in sub_ids[:25]:
    if not cid or not _UUID_RE.match(cid):
      continue
    tmeta = _summarize_transcript_fast(cid)
    if not tmeta.get("exists"):
      out[cid] = {"exists": False, "state": "unknown"}
      continue
    st = tmeta.get("status", "IDLE")
    out[cid] = {
        "exists": True, "state": "running" if st == "RUNNING" else ("errored" if st == "ERROR" else "completed"),
        "steps": tmeta.get("stepCount", 0), "tool_calls": tmeta.get("toolCallCount", 0),
        "last_tool": tmeta.get("lastAction") or "",
        "age_seconds": round(max(0.0, time.time() - (tmeta.get("mtime") or time.time())), 1)
    }
  return out


def _check_tracer_git_update(force: bool = False) -> dict:
  now = time.time()
  if not force and _UPDATE_CACHE["data"] and (now - _UPDATE_CACHE["ts"] < 300.0):
    return _UPDATE_CACHE["data"]
  try:
    subprocess.run(["git", "-C", TRACER_DIR, "fetch", "--quiet"], timeout=10, check=True, capture_output=True)
    local = subprocess.check_output(["git", "-C", TRACER_DIR, "rev-parse", "HEAD"], text=True, timeout=5).strip()[:7]
    remote = subprocess.check_output(["git", "-C", TRACER_DIR, "rev-parse", "@{u}"], text=True, timeout=5).strip()[:7]
    behind = int(subprocess.check_output(["git", "-C", TRACER_DIR, "rev-list", "--count", "HEAD..@{u}"], text=True, timeout=5).strip())
    result = {"supported": True, "update_available": behind > 0, "behind": behind, "local": local, "local_sha": local, "remote": remote, "remote_sha": remote, "dirty": False, "commits": []}
  except Exception as e:
    result = {"supported": False, "update_available": False, "behind": 0, "error": str(e)}
  _UPDATE_CACHE.update({"data": result, "ts": now})
  return result


def _render_embedded_tracer_html(conv_id: str) -> bytes:
  raw_bytes = _read_cached_file_bytes(os.path.join(TRACER_DIR, "index.html"))
  if raw_bytes is None:
    return b"<html><body style='background:#0d1117;color:#e6edf3;font-family:sans-serif;padding:24px'>Agent Tracer index.html not found.</body></html>"
  html = raw_bytes.decode("utf-8", errors="replace")
  bridge_script = f"""
<style>
  html.harness-compact-embed .header, html.harness-compact-embed .goalbar,
  html.harness-compact-embed #right, html.harness-compact-embed #right-bar,
  html.harness-compact-embed .topo-footer, html.harness-compact-embed .topo-hud-inspect {{ display: none !important; }}
  html.harness-compact-embed #left {{ flex: 1 1 100% !important; width: 100% !important; height: 100% !important; border-right: none !important; }}
  html.harness-compact-embed .swimlane-header {{ font-size: 10.5px !important; padding: 3px 7px !important; }}
  html.harness-compact-embed .topo-hud {{ flex-wrap: nowrap !important; padding: 3px 8px !important; min-height: 26px !important; max-height: 28px !important; gap: 6px !important; overflow: hidden !important; }}
  html.harness-compact-embed .topo-hud-body {{ min-width: 0 !important; }}
  html.harness-compact-embed .topo-stage {{ flex: 1 1 auto !important; min-height: 140px !important; height: 100% !important; }}
</style>
<script>
  (function() {{
    const urlParams = new URLSearchParams(window.location.search);
    const forcedConvId = urlParams.get('conversationId') || {json.dumps(conv_id or "")};
    const uiToken = urlParams.get('token') || "";
    const isCompact = urlParams.get('compact') === '1';
    const initialMode = urlParams.get('mode') || "";
    if (isCompact) document.documentElement.classList.add('harness-compact-embed');
    window.sidecar = window.sidecar || {{}};
    if (forcedConvId) window.sidecar.conversationId = forcedConvId;
    if (uiToken && window.fetch) {{
      const origFetch = window.fetch.bind(window);
      window.fetch = function(resource, init) {{
        const opts = Object.assign({{}}, init || {{}});
        const headers = new Headers(opts.headers || {{}});
        if (!headers.has('X-Sidecar-Token')) headers.set('X-Sidecar-Token', uiToken);
        opts.headers = headers;
        return origFetch(resource, opts);
      }};
    }}
    window.addEventListener('message', function(ev) {{
      const d = ev.data;
      if (!d) return;
      if (d.type === 'HARNESS_SET_CONVERSATION' && d.conversationId && window.sidecar.conversationId !== d.conversationId) {{
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set('conversationId', d.conversationId);
        window.location.replace(nextUrl.toString());
      }} else if (d.type === 'HARNESS_SET_THEME') {{
        const wantDark = d.theme ? (d.theme === 'dark') : !!d.dark;
        if (wantDark !== (document.documentElement.getAttribute('data-theme') === 'dark')) {{
          const btn = document.getElementById('btnTheme');
          if (btn) btn.click();
          else document.documentElement.setAttribute('data-theme', wantDark ? 'dark' : 'light');
        }}
      }} else if (d.type === 'HARNESS_SET_VIEW_MODE' && d.mode) {{
        const btn = document.getElementById(d.mode === 'topology' ? 'btnViewTopology' : 'btnViewTimeline');
        if (btn) btn.click();
      }} else if (d.type === 'HARNESS_FOCUS_STEP' && d.stepIndex != null) {{
        const timelineBtn = document.getElementById('btnViewTimeline');
        if (timelineBtn && document.body.classList.contains('topology-mode')) timelineBtn.click();
        setTimeout(function() {{
          const searchInput = document.getElementById('searchBox');
          if (d.toolName && searchInput) {{
            searchInput.value = d.toolName;
            searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
          }}
        }}, 60);
      }}
    }});
    window.addEventListener('DOMContentLoaded', function() {{
      if (isCompact && document.body) document.body.classList.add('inspector-collapsed');
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
            try {{ window.parent.postMessage({{ type: 'TRACER_THEME_CHANGED', dark: isDark, theme: isDark ? 'dark' : 'light' }}, '*'); }} catch (e) {{}}
          }}, 20);
        }});
      }}
    }});
  }})();
</script>
"""
  return (html.replace("</head>", bridge_script + "\n</head>", 1) if "</head>" in html else bridge_script + html).encode("utf-8")


def _clean_user_input(raw_content: str) -> tuple[str, str]:
  if not raw_content:
    return "", ""
  sm = re.search(r"<SYSTEM_MESSAGE>(.*?)</SYSTEM_MESSAGE>", raw_content, re.DOTALL)
  sys_notice = sm.group(1).strip() if sm else ""
  if um := re.search(r"<USER_REQUEST>(.*?)</USER_REQUEST>", raw_content, re.DOTALL):
    return um.group(1).strip(), sys_notice
  cleaned = re.sub(r"<(ADDITIONAL_METADATA|SYSTEM_MESSAGE|CONTEXT_SUMMARY)>.*?</\1>", "", raw_content, flags=re.DOTALL)
  return cleaned.strip(), sys_notice


def _summarize_thinking_line(thinking: str) -> str:
  lines = [ln.strip() for ln in (thinking or "").strip().splitlines() if ln.strip()]
  if not lines:
    return ""
  first = re.sub(r"^[\*\#\-\s]+|[\*\#\s]+$", "", lines[0]).strip()
  if len(lines) > 1 and len(first) < 55 and not first.endswith((".", "!", "?", ":")):
    first = f"{first} — {re.sub(r'^[\*\#\-\s]+', '', lines[1]).strip()}"
  return first[:110] + ("…" if len(first) > 110 else "")


def _parse_tool_result_step(res_step: dict) -> dict:
  raw, status = res_step.get("content") or "", res_step.get("status") or "DONE"
  created_iso = completed_iso = ""
  body_lines, in_preamble = [], True
  for line in raw.splitlines():
    if in_preamble and line.startswith("Created At:"):
      created_iso = line.split("Created At:", 1)[1].strip()
    elif in_preamble and line.startswith("Completed At:"):
      completed_iso = line.split("Completed At:", 1)[1].strip()
    elif in_preamble and not line.strip():
      in_preamble = False
    else:
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

  hdr = body.split("Output:", 1)[0] if "Output:" in body else body[:400]
  is_err = status == "ERROR" or bool(re.search(r"The command exited with code [1-9]\d*", hdr)) or body.startswith("Encountered error in tool execution:")
  return {
      "resultStepIndex": res_step.get("step_index"), "status": "ERROR" if is_err else status,
      "durationMs": duration_ms, "outputPreview": body[:1200],
      "isTruncated": len(body) > 1200 or bool(res_step.get("truncated_fields"))
  }


def _clean_arg_value(val):
  if isinstance(val, str):
    s = val.strip()
    return s[1:-1].strip() if (len(s) >= 2 and s[0] == '"' and s[-1] == '"') else s
  if isinstance(val, dict):
    return {k: _clean_arg_value(v) for k, v in val.items()}
  return [_clean_arg_value(x) for x in val] if isinstance(val, list) else val


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
    return f"{_clean_arg_value(args.get('ServerName', ''))} · {_clean_arg_value(args.get('ToolName', ''))}".strip(" ·")
  if name == "invoke_subagent" and isinstance(subs := args.get("Subagents"), list) and subs and isinstance(subs[0], dict):
    return str(_clean_arg_value(subs[0].get("Role") or subs[0].get("TypeName") or "Subagent"))
  return ""


def _build_chat_stream(conv_id: str) -> dict:
  if not conv_id:
    return {"conversationId": "", "items": [], "subagents": [], "stepCount": 0}
  bundle = _get_cached_transcript_bundle(conv_id)
  mtime_ns, fsize = bundle["mtime_ns"], bundle["size"]
  if (cached := _lru_get(_CHAT_STREAM_CACHE, conv_id)) and mtime_ns > 0 and cached[0] == mtime_ns and cached[1] == fsize:
    return cached[2]

  steps = _read_transcript_steps(conv_id, since=-1)
  tok_info = _get_token_telemetry(conv_id, include_generations=True)
  gen_by_step = {g["stepIndex"]: g for g in (tok_info.get("generations") or []) if g.get("stepIndex") is not None}

  items, subagents, n, i = [], [], len(steps), 0
  while i < n:
    st = steps[i]
    stype, sidx, created = st.get("type", ""), st.get("step_index", i), st.get("created_at", "")
    if stype == "USER_INPUT":
      user_text, sys_notice = _clean_user_input(st.get("content") or "")
      if user_text or sys_notice:
        items.append({"kind": "user", "role": "user", "stepIndex": sidx, "lastStepIndex": sidx, "createdAt": created, "content": user_text, "systemNotice": sys_notice})
      i += 1
      continue

    if stype == "PLANNER_RESPONSE":
      trunc_fields = st.get("truncated_fields") or []
      if "content" in trunc_fields and (full_st := _read_step_full(conv_id, sidx)) and full_st.get("content"):
        st = {**st, "content": full_st["content"]}
      thinking, content = (st.get("thinking") or "").strip(), (st.get("content") or "").strip()
      paired_tools, j = [], i + 1
      for tc in st.get("tool_calls") or []:
        tname = tc.get("name") or "tool"
        targs = tc.get("arguments") or tc.get("args") or {}
        if isinstance(targs, str):
          try:
            targs = json.loads(targs)
          except Exception:
            targs = {"raw": targs}
        targs = _clean_arg_value(targs) if isinstance(targs, dict) else {"raw": str(targs)}
        act = (targs.get("toolAction") or targs.get("toolSummary") if isinstance(targs, dict) else None) or tname
        target_hint = _extract_tool_target(tname, targs)
        res_meta = {"resultStepIndex": None, "status": "RUNNING" if st.get("status") != "DONE" else "DONE", "durationMs": None, "outputPreview": "", "isTruncated": False}
        if j < n and steps[j].get("type") == "GENERIC":
          res_meta = _parse_tool_result_step(steps[j])
          j += 1
        if tname == "invoke_subagent" and isinstance(targs, dict):
          for sub_e in targs.get("Subagents") or []:
            if isinstance(sub_e, dict):
              subagents.append({
                  "stepIndex": sidx, "role": sub_e.get("Role") or sub_e.get("TypeName") or "Subagent",
                  "typeName": sub_e.get("TypeName") or "self", "promptPreview": (sub_e.get("Prompt") or "")[:140],
                  "status": res_meta["status"], "durationMs": res_meta["durationMs"], "createdAt": created
              })
        try:
          args_prev = (json.dumps(targs, indent=2) if isinstance(targs, dict) else str(targs))[:600]
        except Exception:
          args_prev = str(targs)[:600]
        paired_tools.append({
            "id": tc.get("id") or f"tc_{sidx}_{len(paired_tools)}", "stepIndex": sidx, "name": tname,
            "action": act, "target": target_hint, "summary": act or target_hint or "", "argsPreview": args_prev, **res_meta
        })

      latest_desc = ""
      if paired_tools:
        lt = paired_tools[-1]
        latest_desc = f"{lt['action']} ({lt['target']})" if (lt.get("target") and lt.get("action") and lt["target"] not in lt["action"]) else (lt.get("action") or lt.get("target") or lt["name"])

      gen_stat = gen_by_step.get(sidx)
      if items and items[-1].get("kind") == "assistant":
        prev = items[-1]
        prev.update({"lastStepIndex": sidx, "updatedAt": created, "status": st.get("status") or prev["status"]})
        if thinking:
          prev["thinking"] = (f"{prev['thinking']}\n\n---\n\n{thinking}" if prev.get("thinking") else thinking)[-3600:]
          prev["thinkingSummary"] = _summarize_thinking_line(thinking)
        if latest_desc or thinking:
          prev["latestAction"] = latest_desc or _summarize_thinking_line(thinking)
        if content:
          prev["content"] = f"{prev['content']}\n\n{content}" if prev.get("content") else content
        prev["toolCalls"].extend(paired_tools)
        if gen_stat:
          prev["tokenTurn"] = {**gen_stat, "outputTokens": (prev["tokenTurn"].get("outputTokens") or 0) + (gen_stat.get("outputTokens") or 0)} if isinstance(prev.get("tokenTurn"), dict) else gen_stat
      else:
        items.append({
            "kind": "assistant", "role": "agent", "stepIndex": sidx, "lastStepIndex": sidx,
            "createdAt": created, "updatedAt": created, "status": st.get("status") or "DONE",
            "thinking": thinking[-3600:], "thinkingSummary": _summarize_thinking_line(thinking),
            "latestAction": latest_desc or _summarize_thinking_line(thinking),
            "thinkingTruncated": "thinking" in trunc_fields, "content": content,
            "toolCalls": paired_tools, "tokenTurn": gen_stat
        })
      i = j
      continue

    if stype == "ERROR_MESSAGE":
      err_c = (st.get("content") or "Agent error occurred").strip()
      if "plan model not specified" not in err_c and "The stream was interrupted" not in err_c:
        items.append({"kind": "error", "role": "agent", "stepIndex": sidx, "lastStepIndex": sidx, "createdAt": created, "content": err_c, "toolCalls": []})
    i += 1

  result = {"conversationId": conv_id, "stepCount": len(steps), "items": items[-120:], "subagents": subagents[-20:]}
  _lru_set(_CHAT_STREAM_CACHE, conv_id, (mtime_ns, fsize, result))
  return result


def _run_agentapi(args: list[str], project_id: str | None = None) -> dict:
  env = os.environ.copy()
  if project_id:
    env["ANTIGRAVITY_PROJECT_ID"] = project_id
  res = subprocess.run(["agentapi"] + args, capture_output=True, text=True, env=env, timeout=25)
  if res.returncode != 0:
    raise RuntimeError(res.stderr.strip() or f"agentapi exited with {res.returncode}")
  out = res.stdout.strip()
  try:
    return json.loads(out) if out else {"ok": True}
  except json.JSONDecodeError:
    return {"ok": True, "raw": out}


def _resolve_plan_model(conv_id: str = "", model_tier: str = "pro") -> str:
  if conv_id:
    if (cached := _lru_get(_TOKEN_USAGE_CACHE, conv_id)) and isinstance(cached[2], dict):
      if (m := cached[2].get("rawModel") or cached[2].get("model") or "").startswith("MODEL_"):
        return m
    traj_resp = _call_ls("GetCascadeTrajectory", {"cascade_id": conv_id}, timeout=2.0) or {}
    for gm in reversed((traj_resp.get("trajectory") or {}).get("generatorMetadata") or []):
      cm = gm.get("chatModel") or {}
      if (m := cm.get("model") or (cm.get("usage") or {}).get("model") or "").startswith("MODEL_"):
        return m

  cfg_resp = _call_ls("GetCascadeModelConfigData", {}, timeout=2.0) or {}
  if (m := ((cfg_resp.get("defaultOverrideModelConfig") or {}).get("modelOrAlias") or {}).get("model") or "").startswith("MODEL_"):
    return m

  models_resp = _call_ls("GetAvailableModels", {}, timeout=2.5) or {}
  rm = models_resp.get("response") or {}
  tiered, models_map = rm.get("tieredModelIds") or {}, rm.get("models") or {}
  tier_key = "pro" if model_tier == "pro" else ("flashLite" if model_tier == "flash_lite" else "flash")
  tier_list = tiered.get(tier_key) or tiered.get("pro") or tiered.get("flash") or []
  if tier_list and (m := (models_map.get(tier_list[0]) or {}).get("model") or "").startswith("MODEL_"):
    return m
  return "MODEL_PLACEHOLDER_M37"


def _planner_cascade_config(resolved_model: str) -> dict:
  return {"plannerConfig": {"planModel": resolved_model, "conversational": {"plannerMode": "CONVERSATIONAL_PLANNER_MODE_DEFAULT"}}}


def _send_message_direct(conv_id: str, message: str, title: str = "", project_id: str | None = None) -> dict:
  resolved_model = _resolve_plan_model(conv_id=conv_id)
  rpc_req = {
      "cascadeId": conv_id, "items": [{"text": message}], "blocking": False,
      "messageOrigin": "AGENT_MESSAGE_ORIGIN_IDE", "cascadeConfig": _planner_cascade_config(resolved_model)
  }
  if (resp := _call_ls("SendUserCascadeMessage", rpc_req, timeout=4.0)) is not None:
    return {"ok": True, "method": "SendUserCascadeMessage", "conversationId": conv_id, "model": resolved_model, "response": resp}

  agent_req = {"recipient": conv_id, "content": message, **({"displayTitle": title} if title else {})}
  if (resp2 := _call_ls("SendAgentMessage", agent_req, timeout=4.0)) is not None:
    return {"ok": True, "method": "SendAgentMessage", "conversationId": conv_id, "response": resp2}

  return _run_agentapi(["send-message", *(["--title", title] if title else []), "--", conv_id, message], project_id=project_id)


def _start_conversation_direct(message: str, title: str = "", model_tier: str = "pro", project_id: str | None = None) -> dict:
  resolved_model = _resolve_plan_model(conv_id="", model_tier=model_tier)
  start_req = {
      "source": "CORTEX_TRAJECTORY_SOURCE_CASCADE_CLIENT", "trajectoryType": "CORTEX_TRAJECTORY_TYPE_CASCADE",
      "customAgentSpec": {
          "codingAgent": {"googleMode": True}, "commandExecutionPolicy": "eager",
          "enforcedWorkspaceValidation": False, "cascadeConfig": _planner_cascade_config(resolved_model)
      },
      "projectEnvConfig": {"projectId": project_id or os.environ.get("ANTIGRAVITY_PROJECT_ID", ""), "defaultProjectEnvironment": {}}
  }
  start_resp = _call_ls("StartCascade", start_req, timeout=4.0)
  if start_resp and (cid := start_resp.get("cascadeId")):
    if title:
      _call_ls("UpdateConversationAnnotations", {"cascadeIds": [cid], "annotations": {"title": title}, "mergeAnnotations": True}, timeout=2.5)
    _call_ls("SendUserCascadeMessage", {
        "cascadeId": cid, "items": [{"text": message}], "blocking": False,
        "messageOrigin": "AGENT_MESSAGE_ORIGIN_IDE", "cascadeConfig": _planner_cascade_config(resolved_model)
    }, timeout=4.0)
    return {"ok": True, "conversationId": cid, "model": resolved_model, "method": "StartCascade"}

  return _run_agentapi(["new-conversation", *(["--title", title] if title else []), "--", message], project_id=project_id)


def _trigger_automation_by_id(sidecar_id: str) -> tuple[dict, int]:
  if not sidecar_id or "/" in sidecar_id or ".." in sidecar_id:
    return {"ok": False, "error": "Invalid automation ID"}, 400
  sjson_path = os.path.join(CONFIG_DIR, "sidecars", sidecar_id, "sidecar.json")
  if not os.path.isfile(sjson_path):
    return {"ok": False, "error": f"Automation {sidecar_id} not found"}, 404
  try:
    with open(sjson_path, "r", encoding="utf-8") as f:
      args = json.load(f).get("args") or []
    if len(args) >= 5 and args[1] == "agentapi" and args[2] == "send-message":
      return _send_message_direct(args[3], args[4]), 200
    if len(args) >= 4 and args[1] == "agentapi" and args[2] == "new-conversation":
      return _start_conversation_direct(args[-1], title=sidecar_id), 200
    return {"ok": False, "error": "Unsupported automation command shape"}, 400
  except Exception as e:
    return {"ok": False, "error": str(e)}, 500


def _resolve_request_conv_id(params: dict | None = None, body: dict | None = None) -> str:
  for key in ("convId", "conversationId", "conv_id"):
    if params and (val := (params.get(key) or [None])[0]):
      return val.strip()
    if body and isinstance(val := body.get(key), str) and val.strip():
      return val.strip()
  return (os.environ.get("ANTIGRAVITY_SIDECAR_CONVERSATION_ID") or os.environ.get("ANTIGRAVITY_CONVERSATION_ID") or "").strip()


class HarnessRequestHandler(BaseHTTPRequestHandler):

  def log_message(self, fmt, *args):
    pass

  def _send_bytes(self, body: bytes, content_type: str, status: int = 200):
    self.send_response(status)
    self.send_header("Content-Type", content_type)
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Cache-Control", "no-store")
    self.send_header("Access-Control-Allow-Origin", "*")
    self.end_headers()
    try:
      self.wfile.write(body)
    except (BrokenPipeError, ConnectionResetError):
      pass

  def _send_json(self, data: dict | list, status: int = 200):
    self._send_bytes(json.dumps(data, separators=(",", ":")).encode("utf-8"), "application/json; charset=utf-8", status=status)

  def _serve_static(self, filename: str, content_type: str):
    body = _read_cached_file_bytes(os.path.join(BASE_DIR, filename))
    if body is None:
      self._send_json({"error": f"{filename} not found"}, status=404)
    else:
      self._send_bytes(body, content_type)

  def do_GET(self):
    parsed = urllib.parse.urlparse(self.path)
    path, params = parsed.path, urllib.parse.parse_qs(parsed.query)

    if path in ("/", "/index.html", "/fullscreen"):
      return self._serve_static("index.html", "text/html; charset=utf-8")
    if path == "/styles.css":
      return self._serve_static("styles.css", "text/css; charset=utf-8")
    if path == "/app.js":
      return self._serve_static("app.js", "application/javascript; charset=utf-8")
    if path == "/preload.js":
      body = _read_cached_file_bytes(PRELOAD_SDK_PATH) or b"window.sidecar = window.sidecar || {};"
      return self._send_bytes(body, "application/javascript; charset=utf-8")
    if path == "/tracer":
      return self._send_bytes(_render_embedded_tracer_html(_resolve_request_conv_id(params=params)), "text/html; charset=utf-8")

    if path == "/api/transcript":
      conv_id = _resolve_request_conv_id(params=params)
      if not conv_id:
        return self._send_json([])
      try:
        since = int(params.get("since", ["-1"])[0])
      except ValueError:
        since = -1
      return self._send_json(_read_transcript_steps(conv_id, since=since))

    if path == "/api/step_full":
      conv_id, step_idx = _resolve_request_conv_id(params=params), params.get("step", [None])[0]
      if not conv_id or step_idx is None:
        return self._send_json({}, status=400)
      try:
        step = _read_step_full(conv_id, int(step_idx))
      except ValueError:
        step = None
      return self._send_json(step or {})

    if path == "/api/subagents_status":
      raw_ids = params.get("ids", [""])[0]
      return self._send_json(_get_subagents_live_status([x.strip() for x in raw_ids.split(",") if x.strip()]))

    if path == "/api/telemetry":
      conv_id = _resolve_request_conv_id(params=params)
      if not conv_id:
        return self._send_json({"available": False, "reason": "No conversationId"})
      tok = _get_token_telemetry(conv_id, include_generations=True)
      return self._send_json({
          "available": True, "conversationId": conv_id, "status": tok.get("lsStatus") or "IDLE",
          "model": tok.get("model") or "Gemini Next", "llmCalls": tok.get("llmCalls", 0),
          "totalInputTokens": tok.get("inputTokens", 0), "totalOutputTokens": tok.get("outputTokens", 0),
          "totalThinkingTokens": tok.get("thinkingTokens", 0), "totalCacheReadTokens": tok.get("cacheReadTokens", 0),
          "totalTokens": tok.get("totalTokens", 0), "cacheHitPct": tok.get("cacheHitRatePct", 0.0),
          "estCostUsd": tok.get("estimatedCostUsd", 0.0), "turns": tok.get("generations") or []
      })

    if path == "/api/update-status":
      return self._send_json(_check_tracer_git_update(force=params.get("force", ["0"])[0] == "1"))

    if path == "/api/conversations":
      host_cid = _resolve_request_conv_id(params=params)
      convs, _ = _list_conversations(host_cid)
      return self._send_json({"conversations": convs, "hostConversationId": host_cid})

    if path in ("/api/harness/chat", "/api/chat_stream"):
      conv_id = _resolve_request_conv_id(params=params)
      return self._send_json(_build_chat_stream(conv_id) if conv_id else {"conversationId": "", "items": [], "stepCount": 0})

    if path in ("/api/harness/overview", "/api/state"):
      host_conv_id = os.environ.get("ANTIGRAVITY_SIDECAR_CONVERSATION_ID") or os.environ.get("ANTIGRAVITY_CONVERSATION_ID") or ""
      req_conv = _resolve_request_conv_id(params=params)
      conversations, global_tokens = _list_conversations(req_conv)
      active_conv_id = req_conv or (conversations[0]["id"] if conversations else "")
      token_telemetry = _get_token_telemetry(active_conv_id, include_generations=True) if active_conv_id else {}

      active_conv_obj = None
      for c in conversations:
        if c["id"] == active_conv_id:
          c["tokens"] = token_telemetry
          active_conv_obj = c

      chat_stream = _build_chat_stream(active_conv_id) if active_conv_id else {"conversationId": "", "items": [], "stepCount": 0}
      all_services = _list_automations_and_sidecars()
      runtime = _get_runtime_and_mcp_status()

      cron_items, sidecar_items = [], []
      for svc in all_services:
        if svc.get("kind") in ("cron-automation", "daemon") and not svc.get("hasWebUi"):
          is_paused = svc.get("restartPolicy") == "never" and not svc.get("isRunning")
          cron_items.append({
              **svc, "plugin": svc.get("id", ""), "name": svc.get("displayName") or svc.get("id", ""),
              "cron": svc.get("scheduleSgt") or svc.get("cronUtc") or "Continuous",
              "status": "PAUSED" if is_paused else ("ACTIVE" if svc.get("isRunning") else "STOPPED"),
              "lastFired": (svc.get("recentLogs") or [""])[-1][:60] if svc.get("recentLogs") else "Scheduled",
              "canTogglePause": svc.get("sourceType") == "user-sidecar"
          })
        else:
          parts = (svc.get("id") or "").split("/", 1)
          sidecar_items.append({
              **svc, "plugin": parts[0], "sidecar": parts[1] if len(parts) > 1 else parts[0],
              "title": svc.get("displayName") or svc.get("id", ""), "type": svc.get("kind") or "ui-plugin",
              "status": "RUNNING" if svc.get("isRunning") else "STOPPED"
          })

      active_status = active_conv_obj["status"] if active_conv_obj else (token_telemetry.get("lsStatus") or "IDLE")
      subagents_list = chat_stream.get("subagents") or []
      return self._send_json({
          "activeConversationId": active_conv_id,
          "languageServer": runtime.get("languageServer") or {},
          "conversations": {"activeId": active_conv_id, "hostActiveId": host_conv_id, "list": conversations},
          "chat": {
              "conversationId": active_conv_id, "status": active_status,
              "totalSteps": max(chat_stream.get("stepCount", 0), active_conv_obj.get("stepCount", 0) if active_conv_obj else 0),
              "subagentCount": max(active_conv_obj.get("subagentCount", 0) if active_conv_obj else 0, len(subagents_list)),
              "subagents": subagents_list, "items": chat_stream.get("items") or []
          },
          "tokens": {
              "activeSession": {
                  **token_telemetry, "cacheHitPct": token_telemetry.get("cacheHitRatePct", 0.0),
                  "estCostUsd": token_telemetry.get("estimatedCostUsd", 0.0),
                  "turnCount": token_telemetry.get("llmCalls", 0), "turns": token_telemetry.get("generations") or []
              },
              "globalRecent": {
                  **global_tokens, "sessionCount": global_tokens.get("sessionsCounted", 0),
                  "estCostUsd": global_tokens.get("estimatedCostUsd", 0.0)
              }
          },
          "automations": {"activeCount": sum(1 for x in cron_items if x["status"] == "ACTIVE"), "items": cron_items},
          "sidecars": {"activeCount": sum(1 for x in sidecar_items if x["status"] == "RUNNING"), "items": sidecar_items},
          "mcp": {"servers": [{**m, "lazyCount": m.get("toolCount", 0)} for m in (runtime.get("mcpServers") or [])]},
          "memories": {"items": runtime.get("recentMemories") or []}
      })

    self._send_json({"error": "Not found"}, status=404)

  def do_POST(self):
    path = urllib.parse.urlparse(self.path).path
    try:
      length = int(self.headers.get("Content-Length", "0"))
      raw_body = self.rfile.read(length) if length > 0 else b"{}"
      data = json.loads(raw_body.decode("utf-8")) if raw_body else {}
    except Exception as e:
      return self._send_json({"error": f"Invalid JSON: {e}"}, status=400)

    # Normalize legacy /api/action into standard route paths
    if path == "/api/action":
      action = (data.get("action") or "").strip()
      path = {
          "send_message": "/_sidecar/send-message",
          "start_conversation": "/_sidecar/new-conversation",
          "stop_conversation": "/api/chat/stop",
          "trigger_automation": "/api/automation/trigger"
      }.get(action, "")
      if not path:
        return self._send_json({"ok": False, "error": f"Unknown action: {action}"}, status=400)

    if path in ("/api/chat/send", "/_sidecar/send-message", "/api/harness/send-message", "/_sidecar/new-conversation", "/api/harness/new-conversation"):
      prompt = (data.get("prompt") or data.get("message") or "").strip()
      if not prompt:
        return self._send_json({"ok": False, "error": "Missing prompt"}, status=400)
      force_new = path in ("/_sidecar/new-conversation", "/api/harness/new-conversation")
      conv_id = "" if force_new else (data.get("convId") or data.get("conversationId") or "").strip()
      if path in ("/_sidecar/send-message", "/api/harness/send-message") and not conv_id:
        conv_id = _resolve_request_conv_id(body=data)
        if not conv_id:
          return self._send_json({"ok": False, "error": "Missing prompt or conversationId"}, status=400)
      try:
        res = (
            _send_message_direct(conv_id, prompt, title=data.get("title") or "", project_id=data.get("projectId"))
            if conv_id
            else _start_conversation_direct(prompt, title=data.get("title") or "", model_tier=data.get("model") or "pro", project_id=data.get("projectId"))
        )
        return self._send_json(res)
      except Exception as e:
        return self._send_json({"ok": False, "error": str(e)}, status=500)

    if path in ("/api/chat/stop", "/api/harness/cancel"):
      conv_id = _resolve_request_conv_id(body=data)
      if not conv_id:
        return self._send_json({"ok": False, "error": "Missing convId"}, status=400)
      return self._send_json({"ok": True, "response": _call_ls("CancelCascadeInvocation", {"cascadeId": conv_id}, timeout=3.0)})

    if path == "/api/automation/toggle":
      sidecar_id = (data.get("plugin") or data.get("sidecarId") or "").strip()
      if not sidecar_id or "/" in sidecar_id or ".." in sidecar_id:
        return self._send_json({"ok": False, "error": "Invalid sidecarId"}, status=400)
      sjson_path = os.path.join(CONFIG_DIR, "sidecars", sidecar_id, "sidecar.json")
      if not os.path.isfile(sjson_path):
        return self._send_json({"ok": False, "error": f"Automation {sidecar_id} not found"}, status=404)
      try:
        with open(sjson_path, "r", encoding="utf-8") as f:
          cfg = json.load(f)
        new_policy = "never" if (cfg.get("restart_policy") or "always") == "always" else "always"
        cfg["restart_policy"] = new_policy
        with open(sjson_path, "w", encoding="utf-8") as f:
          json.dump(cfg, f, indent=2)
          f.write("\n")
        _AUTOMATIONS_CACHE["ts"] = 0.0
        return self._send_json({"ok": True, "plugin": sidecar_id, "restartPolicy": new_policy})
      except Exception as e:
        return self._send_json({"ok": False, "error": str(e)}, status=500)

    if path == "/api/automation/trigger":
      sidecar_id = (data.get("plugin") or data.get("sidecarId") or "").strip()
      payload, status = _trigger_automation_by_id(sidecar_id)
      return self._send_json(payload, status=status)

    if path == "/api/update":
      try:
        out = subprocess.check_output(["git", "-C", TRACER_DIR, "pull", "--ff-only"], stderr=subprocess.STDOUT, text=True, timeout=15).strip()
        _UPDATE_CACHE["ts"] = 0.0
        return self._send_json({"ok": True, "output": out})
      except Exception as e:
        return self._send_json({"ok": False, "error": str(e)}, status=500)

    if path == "/_sidecar/get-conversation-metadata":
      conv_id = _resolve_request_conv_id(body=data)
      if not conv_id:
        return self._send_json({"error": "Missing conversationId"}, status=400)
      resp = _call_ls("GetConversationMetadata", {"conversationId": conv_id}, timeout=2.5)
      metadata = (resp or {}).get("metadata") if isinstance(resp, dict) else {}
      return self._send_json({"response": {"conversationMetadata": {"metadata": metadata or {}}}})

    self._send_json({"error": "Not found"}, status=404)


def main():
  port = int(os.environ.get("ANTIGRAVITY_SIDECAR_WEB_PORT") or os.environ.get("PORT") or "8765")
  server = ThreadingHTTPServer(("0.0.0.0", port), HarnessRequestHandler)
  print(f"[jetski-harness] Custom Harness v2.9 listening on http://0.0.0.0:{port}", flush=True)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
  finally:
    server.server_close()


if __name__ == "__main__":
  main()
