// Jetski Harness Frontend — Resizable Overview Bento Grid + 4 Focused Pillars
(function () {
  "use strict";

  const urlParams = new URLSearchParams(window.location.search);
  const sidecarToken = urlParams.get("token") || "";

  const DEFAULT_LAYOUT = {
    colSplit: 50,
    rowSplit: 54,
    order: ["chat", "tracer", "tokens", "automations"],
    wideCards: [],
  };

  function loadSavedLayout() {
    try {
      const raw = localStorage.getItem("jetski_harness_layout");
      if (!raw) return { ...DEFAULT_LAYOUT, order: [...DEFAULT_LAYOUT.order], wideCards: [] };
      const parsed = JSON.parse(raw);
      const colSplit = Math.min(78, Math.max(22, Number(parsed.colSplit) || DEFAULT_LAYOUT.colSplit));
      const rowSplit = Math.min(78, Math.max(24, Number(parsed.rowSplit) || DEFAULT_LAYOUT.rowSplit));
      const validIds = new Set(DEFAULT_LAYOUT.order);
      const order = Array.isArray(parsed.order)
        ? parsed.order.filter((id) => validIds.has(id))
        : [];
      DEFAULT_LAYOUT.order.forEach((id) => {
        if (!order.includes(id)) order.push(id);
      });
      const wideCards = Array.isArray(parsed.wideCards)
        ? parsed.wideCards.filter((id) => validIds.has(id))
        : [];
      return { colSplit, rowSplit, order, wideCards };
    } catch (_) {
      return { ...DEFAULT_LAYOUT, order: [...DEFAULT_LAYOUT.order], wideCards: [] };
    }
  }

  const state = {
    theme: localStorage.getItem("jetski_harness_theme") || "light",
    activeTab: "overview",
    activeConvId: "",
    overviewTracerMode: "timeline", // "timeline" | "topology"
    selectedMetric: "", // "session_tokens" | "cache_hit_rate" | "session_cost" | "global_tokens" | "context_saturation" | "turn_bars" | ""
    selectedAutomation: "", // plugin/sidecar ID for automation explainer drawer
    layout: loadSavedLayout(),
    dragSourceCardId: "",
    data: null,
    lastChatHash: "",
    lastSelectSig: "",
    sendingPrompt: false,
    fetchSeq: 0,
  };

  // Authenticated fetch helper that always sends X-Sidecar-Token (works in Aux Pane & Full Screen Tab)
  function apiFetch(url, options = {}) {
    if (window.sidecar && typeof window.sidecar.fetch === "function") {
      return window.sidecar.fetch(url, options);
    }
    const headers = new Headers(options.headers || {});
    if (!headers.has("Content-Type") && options.body) {
      headers.set("Content-Type", "application/json");
    }
    if (sidecarToken) {
      headers.set("X-Sidecar-Token", sidecarToken);
    }
    return fetch(url, { ...options, headers });
  }

  // =========================================================================
  // 1. Theme, View Mode & Conversation Sync with Embedded Agent Tracer Iframes
  // =========================================================================
  function applyTheme(theme, broadcast = true) {
    state.theme = theme === "dark" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", state.theme);
    localStorage.setItem("jetski_harness_theme", state.theme);

    const iconEl = document.getElementById("theme-icon");
    const labelEl = document.getElementById("theme-label");
    if (iconEl && labelEl) {
      if (state.theme === "dark") {
        iconEl.textContent = "☀";
        labelEl.textContent = "Light";
      } else {
        iconEl.textContent = "☾";
        labelEl.textContent = "Dark";
      }
    }

    if (broadcast) {
      ["tracer-iframe", "overview-tracer-iframe"].forEach((id) => {
        const iframe = document.getElementById(id);
        if (iframe && iframe.contentWindow) {
          iframe.contentWindow.postMessage(
            { type: "HARNESS_SET_THEME", theme: state.theme },
            "*"
          );
        }
      });
    }
  }

  function broadcastTracerConversation(convId) {
    if (!convId) return;
    ["tracer-iframe", "overview-tracer-iframe"].forEach((id) => {
      const iframe = document.getElementById(id);
      if (iframe && iframe.contentWindow) {
        iframe.contentWindow.postMessage(
          { type: "HARNESS_SET_CONVERSATION", conversationId: convId },
          "*"
        );
      }
    });
  }

  function setOverviewTracerMode(mode) {
    state.overviewTracerMode = mode === "topology" ? "topology" : "timeline";
    document.querySelectorAll(".js-tracer-mode").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-mode") === state.overviewTracerMode);
    });
    const iframe = document.getElementById("overview-tracer-iframe");
    if (iframe && iframe.contentWindow) {
      iframe.contentWindow.postMessage(
        { type: "HARNESS_SET_VIEW_MODE", mode: state.overviewTracerMode },
        "*"
      );
    }
  }

  function focusStepInTracer(stepIndex, toolName) {
    ["overview-tracer-iframe", "tracer-iframe"].forEach((id) => {
      const iframe = document.getElementById(id);
      if (iframe && iframe.contentWindow) {
        iframe.contentWindow.postMessage(
          {
            type: "HARNESS_FOCUS_STEP",
            stepIndex: Number(stepIndex) || 0,
            toolName: toolName || "",
          },
          "*"
        );
      }
    });

    // Visual highlight feedback on the Agent Tracer Bento card
    const tracerCard = document.getElementById("bento-card-tracer");
    if (tracerCard) {
      tracerCard.classList.add("tracer-flash");
      setTimeout(() => tracerCard.classList.remove("tracer-flash"), 900);
    }
  }

  function mountTracerIframe(id) {
    const iframe = document.getElementById(id);
    if (!iframe || iframe.dataset.mounted === "1") return;
    iframe.dataset.mounted = "1";
    const isCompact = id === "overview-tracer-iframe";
    const q = new URLSearchParams();
    if (isCompact) q.set("compact", "1");
    if (sidecarToken) q.set("token", sidecarToken);
    if (state.activeConvId) q.set("conversationId", state.activeConvId);
    const qs = q.toString();
    iframe.src = qs ? `tracer?${qs}` : "tracer";

    iframe.addEventListener("load", () => {
      applyTheme(state.theme, true);
      if (state.activeConvId) {
        broadcastTracerConversation(state.activeConvId);
      }
      if (isCompact && state.overviewTracerMode === "topology") {
        setOverviewTracerMode("topology");
      }
    });
  }

  function initTracerIframes() {
    mountTracerIframe("overview-tracer-iframe");
  }

  window.addEventListener("message", (e) => {
    if (e.data && e.data.type === "TRACER_THEME_CHANGED") {
      if (e.data.theme && e.data.theme !== state.theme) {
        applyTheme(e.data.theme, true);
      }
    }
  });

  // =========================================================================
  // 2. Resizable & Drag-and-Drop Overview Bento Grid + Reset Layout
  // =========================================================================
  function isLayoutCustomized() {
    const l = state.layout;
    if (Math.abs(l.colSplit - DEFAULT_LAYOUT.colSplit) > 0.5) return true;
    if (Math.abs(l.rowSplit - DEFAULT_LAYOUT.rowSplit) > 0.5) return true;
    if (l.wideCards.length > 0) return true;
    return l.order.some((id, idx) => id !== DEFAULT_LAYOUT.order[idx]);
  }

  function saveAndApplyLayout() {
    try {
      localStorage.setItem("jetski_harness_layout", JSON.stringify(state.layout));
    } catch (_) {}
    applyGridLayout();
  }

  function applyGridLayout() {
    const grid = document.getElementById("bento-grid");
    if (!grid) return;

    const { colSplit, rowSplit, order, wideCards } = state.layout;
    grid.style.setProperty("--bento-col-split", `${colSplit.toFixed(1)}%`);
    grid.style.setProperty("--bento-row-split", `${rowSplit.toFixed(1)}%`);
    grid.style.setProperty("--bento-col-ratio", String(colSplit / 100));
    grid.style.setProperty("--bento-row-ratio", String(rowSplit / 100));

    // Reorder cards in DOM according to state.layout.order
    order.forEach((cardId) => {
      const card = grid.querySelector(`.bento-card[data-card-id="${cardId}"]`);
      if (card) {
        const isWide = wideCards.includes(cardId);
        card.classList.toggle("is-wide", isWide);
        const wideBtn = card.querySelector(".js-toggle-wide");
        if (wideBtn) {
          wideBtn.classList.toggle("active", isWide);
          wideBtn.title = isWide
            ? "Restore to 1-column quadrant width"
            : "Expand card across both columns (Wide)";
        }
        grid.appendChild(card);
      }
    });

    grid.classList.toggle("has-wide-card", wideCards.length > 0);

    const resetBtn = document.getElementById("btn-reset-layout");
    if (resetBtn) {
      const customized = isLayoutCustomized();
      resetBtn.classList.toggle("layout-customized", customized);
    }

    // Trigger resize on embedded Agent Tracer so SVG/Topology re-centers smoothly
    const ovIframe = document.getElementById("overview-tracer-iframe");
    if (ovIframe && ovIframe.contentWindow) {
      try {
        ovIframe.contentWindow.dispatchEvent(new Event("resize"));
      } catch (_) {}
    }
  }

  function resetGridLayout() {
    state.layout = {
      colSplit: DEFAULT_LAYOUT.colSplit,
      rowSplit: DEFAULT_LAYOUT.rowSplit,
      order: [...DEFAULT_LAYOUT.order],
      wideCards: [],
    };
    saveAndApplyLayout();
    if (state.activeTab !== "overview") {
      switchTab("overview");
    }
  }

  function initBentoGridCustomization() {
    const grid = document.getElementById("bento-grid");
    const colResizer = document.getElementById("bento-col-resizer");
    const rowResizer = document.getElementById("bento-row-resizer");
    if (!grid) return;

    applyGridLayout();

    // 1. Central Column & Row Splitters
    function attachResizer(resizerEl, axis) {
      if (!resizerEl) return;

      resizerEl.addEventListener("dblclick", (e) => {
        e.preventDefault();
        if (axis === "col") {
          state.layout.colSplit = DEFAULT_LAYOUT.colSplit;
        } else {
          state.layout.rowSplit = DEFAULT_LAYOUT.rowSplit;
        }
        saveAndApplyLayout();
      });

      resizerEl.addEventListener("pointerdown", (e) => {
        e.preventDefault();
        resizerEl.classList.add("dragging");
        document.body.classList.add("is-resizing-grid");

        const onMove = (moveEv) => {
          const rect = grid.getBoundingClientRect();
          if (axis === "col") {
            const relX = moveEv.clientX - rect.left;
            const pct = (relX / Math.max(1, rect.width)) * 100;
            state.layout.colSplit = Math.min(76, Math.max(24, pct));
          } else {
            const relY = moveEv.clientY - rect.top;
            const pct = (relY / Math.max(1, rect.height)) * 100;
            state.layout.rowSplit = Math.min(76, Math.max(26, pct));
          }
          applyGridLayout();
        };

        const onUp = () => {
          resizerEl.classList.remove("dragging");
          document.body.classList.remove("is-resizing-grid");
          window.removeEventListener("pointermove", onMove);
          window.removeEventListener("pointerup", onUp);
          saveAndApplyLayout();
        };

        window.addEventListener("pointermove", onMove);
        window.addEventListener("pointerup", onUp);
      });
    }

    attachResizer(colResizer, "col");
    attachResizer(rowResizer, "row");

    // 2. Drag-and-Drop Card Swapping via .card-drag-handle
    grid.querySelectorAll(".card-drag-handle").forEach((handle) => {
      handle.addEventListener("dragstart", (e) => {
        const card = handle.closest(".bento-card");
        if (!card) return;
        const cardId = card.getAttribute("data-card-id") || "";
        state.dragSourceCardId = cardId;
        document.body.classList.add("is-dragging-card");
        if (e.dataTransfer) {
          e.dataTransfer.effectAllowed = "move";
          e.dataTransfer.setData("text/plain", cardId);
        }
      });

      handle.addEventListener("dragend", () => {
        state.dragSourceCardId = "";
        document.body.classList.remove("is-dragging-card");
        grid.querySelectorAll(".bento-card").forEach((c) => c.classList.remove("drag-over"));
      });
    });

    grid.querySelectorAll(".bento-card").forEach((card) => {
      card.addEventListener("dragover", (e) => {
        if (!state.dragSourceCardId) return;
        const targetId = card.getAttribute("data-card-id");
        if (targetId && targetId !== state.dragSourceCardId) {
          e.preventDefault();
          card.classList.add("drag-over");
        }
      });

      card.addEventListener("dragleave", () => {
        card.classList.remove("drag-over");
      });

      card.addEventListener("drop", (e) => {
        e.preventDefault();
        card.classList.remove("drag-over");
        const srcId = state.dragSourceCardId || (e.dataTransfer && e.dataTransfer.getData("text/plain"));
        const dstId = card.getAttribute("data-card-id");
        if (!srcId || !dstId || srcId === dstId) return;

        const order = [...state.layout.order];
        const i1 = order.indexOf(srcId);
        const i2 = order.indexOf(dstId);
        if (i1 >= 0 && i2 >= 0) {
          order[i1] = dstId;
          order[i2] = srcId;
          state.layout.order = order;
          saveAndApplyLayout();
        }
      });
    });

    // 3. Per-Card Wide (2-column span) Toggle
    grid.querySelectorAll(".js-toggle-wide").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const cardId = btn.getAttribute("data-card");
        if (!cardId) return;
        const wide = new Set(state.layout.wideCards);
        if (wide.has(cardId)) {
          wide.delete(cardId);
        } else {
          wide.add(cardId);
        }
        state.layout.wideCards = Array.from(wide);
        saveAndApplyLayout();
      });
    });

    // 4. Reset Layout Button
    const resetBtn = document.getElementById("btn-reset-layout");
    if (resetBtn) {
      resetBtn.addEventListener("click", resetGridLayout);
    }
  }

  // =========================================================================
  // 3. Tab Navigation & Expand Buttons
  // =========================================================================
  function switchTab(tabName) {
    if (!tabName) return;
    state.activeTab = tabName;
    if (tabName === "tracer") {
      mountTracerIframe("tracer-iframe");
    }
    document.querySelectorAll(".tab-btn").forEach((b) => {
      b.classList.toggle("active", b.getAttribute("data-tab") === tabName);
    });
    document.querySelectorAll(".tab-view").forEach((v) => {
      v.classList.toggle("active", v.id === "view-" + tabName);
    });
  }

  function initTabs() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        switchTab(btn.getAttribute("data-tab"));
      });
    });

    document.querySelectorAll("[data-goto-tab]").forEach((btn) => {
      btn.addEventListener("click", () => {
        switchTab(btn.getAttribute("data-goto-tab"));
      });
    });
  }

  // =========================================================================
  // 4. Formatting Helpers
  // =========================================================================
  function fmtTokens(n) {
    const num = Number(n) || 0;
    if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + "M";
    if (num >= 1_000) return (num / 1_000).toFixed(1) + "k";
    return String(num);
  }

  function escapeHtml(str) {
    return String(str || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderMarkdownLite(text) {
    let html = escapeHtml(text);
    html = html.replace(/```(\w*)\n([\s\S]*?)```/g, (_, _lang, code) => {
      return "<pre><code>" + code.trim() + "</code></pre>";
    });
    html = html.replace(/`([^`\n]+)`/g, "<code>$1</code>");
    html = html.replace(/\*\*([^*\n]+)\*\*/g, "<strong>$1</strong>");
    return html;
  }

  function fmtTimeShort(iso) {
    if (!iso) return "";
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    } catch (_) {
      return "";
    }
  }

  function compactCronLabel(cronStr) {
    if (!cronStr) return "Scheduled";
    return String(cronStr)
      .replace(/\s*\([^)]*UTC[^)]*\)/gi, "")
      .replace("Weekdays (Mon–Fri) at ", "Mon–Fri ")
      .replace("Weekdays (Mon-Fri) at ", "Mon–Fri ")
      .replace("Daily at ", "Daily ")
      .replace(" (Daily)", "")
      .replace(" (Daemon Loop)", "")
      .trim();
  }

  // =========================================================================
  // 5. Interactive Metric Explainer (Tokens, Cache, Cost, Context Saturation)
  // =========================================================================
  function buildMetricExplanation(metricKey, data) {
    const tok = (data && data.tokens) || {};
    const active = tok.activeSession || {};
    const global = tok.globalRecent || {};

    const totalTok = active.totalTokens || 0;
    const promptTok = active.inputTokens || 0;
    const cachedTok = active.cacheReadTokens || 0;
    const freshTok = Math.max(0, promptTok - cachedTok);
    const outTok = active.outputTokens || 0;
    const thinkTok = active.thinkingTokens || 0;
    const cachePct = active.cacheHitPct || 0;
    const costUsd = active.estCostUsd || 0;
    const turns = active.turnCount || 0;
    const modelName = active.model || "Gemini Next";
    const ctxWin = active.contextWindowTokens || 0;
    const ctxSatPct = Math.min(100, Math.round((ctxWin / 200_000) * 100));

    const rates = active.pricingRates || {
      inputPer1M: 1.25,
      cachedPer1M: 0.3125,
      outputPer1M: 10.0,
    };
    const inRate = Number(rates.inputPer1M) || 1.25;
    const cacheRate = Number(rates.cachedPer1M) || 0.3125;
    const outRate = Number(rates.outputPer1M) || 10.0;
    const savedUsd = (cachedTok / 1_000_000) * Math.max(0, inRate - cacheRate);

    const specs = {
      session_tokens: {
        title: "Session Tokens — Cumulative Conversation Volume",
        desc: "Total tokens processed across all LLM turns in the selected conversation, combining the cumulative context window sent to the model (Prompt) and the tokens generated back (Output + Thinking).",
        formula: "Session Tokens = Fresh Input + Cached Context Reused + Model Output + Thinking",
        live: `Live breakdown (${turns} turns): ${fmtTokens(freshTok)} fresh input + ${fmtTokens(cachedTok)} cached context + ${fmtTokens(outTok)} output${thinkTok ? ` + ${fmtTokens(thinkTok)} thinking` : ""} = ${fmtTokens(totalTok)} total.`,
      },
      cache_hit_rate: {
        title: "Cache Hit Rate — Context Caching Efficiency",
        desc: "Percentage of prompt tokens served from prefix context cache instead of being re-tokenised from scratch. High cache hit rates (>75%) drastically reduce Time-To-First-Token (TTFT) latency and cut input token cost.",
        formula: "Cache Hit Rate = Cache Read Tokens ÷ (Fresh Input Tokens + Cache Read Tokens)",
        live: `Live breakdown: ${fmtTokens(cachedTok)} of ${fmtTokens(promptTok)} prompt tokens (${cachePct}%) were reused from cache, saving ~$${savedUsd.toFixed(2)} in compute cost this session.`,
      },
      session_cost: {
        title: "Estimated Session Cost — API Equivalent Spend",
        desc: `Estimated dollar equivalent for the active conversation using ${modelName} tier rates ($${inRate}/1M fresh input, $${cacheRate}/1M cached context read, $${outRate}/1M output & thinking tokens).`,
        formula: `Cost = (Fresh × $${inRate}/1M) + (Cached × $${cacheRate}/1M) + ((Output + Thinking) × $${outRate}/1M)`,
        live: `Live breakdown (${modelName}): $${costUsd.toFixed(2)} across ${turns} LLM calls (without context caching, this session would have cost ~$${(costUsd + savedUsd).toFixed(2)}).`,
      },
      global_tokens: {
        title: "Recent Sessions Total — Cross-Session Footprint",
        desc: "Aggregated token volume and estimated cost across your most recent active Jetski conversations on this machine.",
        formula: "Sum of (Prompt + Output + Thinking) across recent active sessions",
        live: `Live breakdown: ${fmtTokens(global.totalTokens || 0)} total tokens across ${global.sessionCount || 0} recent sessions · Est. total $${(global.estCostUsd || 0).toFixed(2)} (${global.cacheHitRatePct || 0}% overall cache hit rate).`,
      },
      context_saturation: {
        title: "Context Window Saturation — Compaction Threshold (~200k)",
        desc: "Measures the latest turn's active prompt context size against Jetski's ~200,000-token context compaction threshold. When a conversation approaches 100%, Jetski summarises earlier turns into a <CONTEXT_SUMMARY> block to keep latency low and prevent context overflow.",
        formula: "Saturation % = Latest Turn Input Tokens ÷ 200,000 Compaction Threshold",
        live: `Current context window: ${fmtTokens(ctxWin)} / 200.0k tokens (${ctxSatPct}% full). ${ctxSatPct >= 85 ? "Approaching compaction threshold." : "Plenty of headroom before context compaction."}`,
      },
      turn_bars: {
        title: "Per-Turn Context Growth Bars — Colour Legend",
        desc: "Each horizontal bar represents one LLM call in the conversation, showing how the context window grows as files are viewed and tools run.",
        formula: "Green = Cached Context Reused · Blue = Fresh Uncached Input · Purple = Model Output",
        live: `Latest turn context window: ${fmtTokens(ctxWin)} tokens (${turns} total turns in this session).`,
      },
    };

    return specs[metricKey] || null;
  }

  function renderMetricExplainers(data) {
    ["session_tokens", "cache_hit_rate", "session_cost", "global_tokens", "context_saturation", "turn_bars"].forEach(
      (key) => {
        const info = buildMetricExplanation(key, data);
        if (!info) return;
        document.querySelectorAll(`.js-metric-card[data-metric="${key}"]`).forEach((el) => {
          el.title = `${info.title}\n${info.desc}\n${info.live}\n(Click to pin/hide details)`;
          el.classList.toggle("active-metric", state.selectedMetric === key);
        });
      }
    );

    const boxes = [
      document.getElementById("bento-metric-explainer"),
      document.getElementById("tokens-metric-explainer"),
    ];

    if (!state.selectedMetric) {
      boxes.forEach((box) => {
        if (box) box.hidden = true;
      });
      return;
    }

    const info = buildMetricExplanation(state.selectedMetric, data);
    if (!info) return;

    const html = `
      <div class="metric-explainer-head">
        <span>${escapeHtml(info.title)}</span>
        <button type="button" class="metric-explainer-close js-close-explainer" title="Close explanation">&times;</button>
      </div>
      <div>${escapeHtml(info.desc)}</div>
      <div class="metric-explainer-formula">${escapeHtml(info.formula)}</div>
      <div class="metric-explainer-live"><strong>Current Session:</strong> ${escapeHtml(info.live)}</div>
    `;

    boxes.forEach((box) => {
      if (!box) return;
      box.innerHTML = html;
      box.hidden = false;
    });
  }

  // =========================================================================
  // 6. Interactive Automation & Cron Job Explainer Drawer
  // =========================================================================
  function buildAutomationExplanation(item) {
    if (!item) return null;
    const id = item.id || item.plugin || "";
    const name = item.name || item.title || item.displayName || id;
    const status = item.status || (item.isRunning ? "ACTIVE" : "STOPPED");
    const scheduleSgt = item.scheduleSgt || item.cron || "Always-On";
    const cronUtc = item.cronUtc || "N/A (Daemon / Web UI)";
    const target = item.targetSummary || item.type || "Sidecar process";

    const purpose =
      item.description ||
      "Background automation or sidecar service registered in your Jetski configuration.";
    const mechanism =
      item.mechanismDesc ||
      `Executed by SidecarManager via \`${target}\` (restart policy: \`${item.restartPolicy || "always"}\`).`;
    const safety =
      item.safetyDesc ||
      "Managed locally on your machine; can be triggered on-demand or paused/resumed via `sidecar.json`.";

    return {
      id,
      name,
      status,
      scheduleSgt,
      cronUtc,
      target,
      extraBadge: item.extraBadge || "",
      purpose,
      mechanism,
      safety,
      promptPreview: item.promptPreview || "",
      canTriggerNow: !!item.canTriggerNow,
      canTogglePause: !!item.canTogglePause,
      restartPolicy: item.restartPolicy || "always",
    };
  }

  function findAutomationItemById(id) {
    if (!state.data || !id) return null;
    const autos = (state.data.automations && state.data.automations.items) || [];
    const sidecars = (state.data.sidecars && state.data.sidecars.items) || [];
    return (
      autos.find((a) => a.id === id || a.plugin === id) ||
      sidecars.find((s) => s.id === id || `${s.plugin}/${s.sidecar}` === id || s.plugin === id) ||
      null
    );
  }

  function renderAutomationExplainers() {
    const boxes = [
      document.getElementById("bento-auto-explainer"),
      document.getElementById("automations-explainer"),
    ];

    document.querySelectorAll("[data-auto-id]").forEach((el) => {
      el.classList.toggle(
        "active-auto",
        !!state.selectedAutomation && el.getAttribute("data-auto-id") === state.selectedAutomation
      );
    });

    if (!state.selectedAutomation) {
      boxes.forEach((b) => {
        if (b) b.hidden = true;
      });
      return;
    }

    const item = findAutomationItemById(state.selectedAutomation);
    const info = buildAutomationExplanation(item);
    if (!info) {
      boxes.forEach((b) => {
        if (b) b.hidden = true;
      });
      return;
    }

    const promptRow = info.promptPreview
      ? `<div class="metric-explainer-live"><strong>Configured Prompt / Workflow:</strong> <code>${escapeHtml(
          info.promptPreview
        )}</code></div>`
      : "";

    const html = `
      <div class="metric-explainer-head">
        <span>${escapeHtml(info.name)} — Automation & Runtime Breakdown</span>
        <button type="button" class="metric-explainer-close js-close-auto-explainer" title="Close explanation">&times;</button>
      </div>
      <div><strong>What it does:</strong> ${escapeHtml(info.purpose)}</div>
      <div class="metric-explainer-formula">Schedule: ${escapeHtml(info.scheduleSgt)}${
      info.cronUtc && info.cronUtc !== "N/A (Daemon / Web UI)"
        ? ` · Raw UTC Cron: ${escapeHtml(info.cronUtc)}`
        : ""
    } · Command: ${escapeHtml(info.target)}</div>
      <div class="metric-explainer-live"><strong>How it runs:</strong> ${escapeHtml(info.mechanism)}</div>
      ${promptRow}
      <div class="metric-explainer-live"><strong>Runtime & Safety:</strong> ${escapeHtml(info.safety)}${
      info.extraBadge ? ` (${escapeHtml(info.extraBadge)})` : ""
    }</div>
    `;

    boxes.forEach((box) => {
      if (!box) return;
      box.innerHTML = html;
      box.hidden = false;
    });
  }

  // =========================================================================
  // 7. Conversation Switching (Instant + Race-Free)
  // =========================================================================
  function selectConversation(convId) {
    if (!convId || convId === state.activeConvId) return;
    state.activeConvId = convId;
    state.lastChatHash = "";

    document.querySelectorAll(".js-session-select").forEach((sel) => {
      if (sel.value !== convId) {
        sel.value = convId;
      }
    });

    broadcastTracerConversation(convId);

    const list = (state.data && state.data.conversations && state.data.conversations.list) || [];
    const chosen = list.find((c) => c.id === convId);
    const chosenTitle = chosen ? chosen.title : convId.slice(0, 8);

    const bentoTitleEl = document.getElementById("bento-chat-title");
    if (bentoTitleEl) {
      bentoTitleEl.textContent = chosenTitle;
    }

    const bentoBox = document.getElementById("bento-chat-messages");
    if (bentoBox) {
      bentoBox.innerHTML = `<div class="empty-state-sm">Loading conversation: <strong>${escapeHtml(chosenTitle)}</strong>...</div>`;
    }
    const fullBox = document.getElementById("chat-messages");
    if (fullBox) {
      fullBox.innerHTML = `<div class="empty-state">Loading conversation: <strong>${escapeHtml(chosenTitle)}</strong>...</div>`;
    }

    fetchState(true);
  }

  function cycleConversation(delta) {
    const list = (state.data && state.data.conversations && state.data.conversations.list) || [];
    if (list.length <= 1) return;
    const idx = list.findIndex((c) => c.id === state.activeConvId);
    const curIdx = idx >= 0 ? idx : 0;
    const nextIdx = (curIdx + delta + list.length) % list.length;
    if (list[nextIdx]) {
      selectConversation(list[nextIdx].id);
    }
  }

  // =========================================================================
  // 8. Render Functions
  // =========================================================================
  function renderTopbarAndPicker(data) {
    const dot = document.getElementById("ls-status-dot");
    if (dot) {
      const online = data.languageServer && data.languageServer.online;
      dot.classList.toggle("online", !!online);
      dot.title = online
        ? `Connected to Jetski Language Server (PID ${data.languageServer.pid})`
        : "Language Server offline (using transcript fallback)";
    }

    const convs = (data.conversations && data.conversations.list) || [];
    if (!state.activeConvId && data.conversations && data.conversations.activeId) {
      state.activeConvId = data.conversations.activeId;
      broadcastTracerConversation(state.activeConvId);
    }

    const chatCountPill = document.getElementById("chat-count-pill");
    if (chatCountPill) {
      chatCountPill.textContent = String(convs.length);
    }

    const selectSig = convs
      .map((c) => `${c.id}:${c.status}:${c.title}:${c.stepCount || 0}`)
      .join("|");

    const optionsHtml =
      convs.length === 0
        ? `<option value="">No conversations found</option>`
        : convs
            .map((c) => {
              const prefix = c.status === "RUNNING" ? "• " : "";
              const title =
                c.title.length > 44 ? c.title.slice(0, 44) + "…" : c.title;
              const stepsSuffix = c.stepCount ? ` (${c.stepCount} steps)` : "";
              return `<option value="${escapeHtml(c.id)}">${prefix}${escapeHtml(title)}${stepsSuffix}</option>`;
            })
            .join("");

    document.querySelectorAll(".js-session-select").forEach((sel) => {
      const isFocused = document.activeElement === sel;
      if (!isFocused && (sel.dataset.sig !== selectSig || sel.options.length === 0)) {
        sel.innerHTML = optionsHtml;
        sel.dataset.sig = selectSig;
      }
      if (state.activeConvId && sel.value !== state.activeConvId) {
        sel.value = state.activeConvId;
      }
    });
    state.lastSelectSig = selectSig;

    // Pills
    const activeAutoCount =
      ((data.automations && data.automations.activeCount) || 0) +
      ((data.sidecars && data.sidecars.activeCount) || 0);
    const autoPill = document.getElementById("auto-count-pill");
    if (autoPill) autoPill.textContent = String(activeAutoCount);

    const activeTok = (data.tokens && data.tokens.activeSession) || {};
    const tokPill = document.getElementById("token-summary-pill");
    if (tokPill) {
      tokPill.textContent = fmtTokens(activeTok.totalTokens || 0);
    }

    const subPill = document.getElementById("tracer-subagent-pill");
    if (subPill) {
      const subCount = (data.chat && data.chat.subagentCount) || 0;
      if (subCount > 0) {
        subPill.hidden = false;
        subPill.textContent = `${subCount} sub`;
      } else {
        subPill.hidden = true;
      }
    }
  }

  function renderSubagentStrips(subagents) {
    const list = Array.isArray(subagents) ? subagents : [];
    const strips = [
      document.getElementById("bento-subagent-strip"),
      document.getElementById("chat-subagent-strip"),
    ];

    if (list.length === 0) {
      strips.forEach((s) => {
        if (s) s.hidden = true;
      });
      return;
    }

    const chipsHtml =
      `<span class="subagent-strip-label">Subagents (${list.length}):</span>` +
      list
        .map((sa) => {
          const isRun = sa.status === "RUNNING";
          const dur = sa.durationMs ? ` · ${(sa.durationMs / 1000).toFixed(1)}s` : "";
          return `
            <button type="button" class="subagent-chip js-focus-step" data-step-index="${
              sa.stepIndex || 0
            }" data-tool-name="invoke_subagent" title="Subagent (${escapeHtml(
            sa.typeName
          )}): ${escapeHtml(sa.promptPreview || "")}\nClick to highlight in Agent Tracer">
              <span class="status-dot-sm ${isRun ? "running" : ""}"></span>
              <span>${escapeHtml(sa.role)}</span>
              <span class="kpi-sub">(${escapeHtml(sa.typeName)}${dur})</span>
            </button>
          `;
        })
        .join("");

    strips.forEach((strip) => {
      if (!strip) return;
      strip.innerHTML = chipsHtml;
      strip.hidden = false;
    });
  }

  function buildSingleToolDetailHtml(tc, defaultStepIdx) {
    const stepIdx = tc.stepIndex || defaultStepIdx || 0;
    const isRun = tc.status === "RUNNING";
    const isErr = tc.status === "ERROR";
    const dotClass = isRun ? "running" : isErr ? "error" : "";
    const targetText = tc.target || tc.action || tc.summary || "";
    const durText =
      tc.durationMs != null ? `${(tc.durationMs / 1000).toFixed(1)}s` : isRun ? "running…" : "";
    const outText =
      tc.outputPreview && tc.outputPreview.trim()
        ? tc.outputPreview
        : isRun
        ? "Running — waiting for tool output..."
        : "Completed (no text stdout)";
    const detailId = `tc-${tc.id || stepIdx}-${tc.name}`;

    return `
      <details class="aux-pill" data-detail-id="${escapeHtml(detailId)}">
        <summary title="Click to view tool output & arguments">
          <div class="aux-pill-summary-left">
            <span class="status-dot-sm ${dotClass}"></span>
            <span class="aux-type-tag">${escapeHtml(tc.name)}</span>
            <span class="tool-target-text">${escapeHtml(targetText)}</span>
          </div>
          <div class="aux-pill-summary-right">
            ${durText ? `<span class="tool-dur">${escapeHtml(durText)}</span>` : ""}
            <button type="button" class="btn-trace-step js-focus-step" data-step-index="${stepIdx}" data-tool-name="${escapeHtml(
      tc.name
    )}" title="Highlight step #${stepIdx} in Agent Tracer">Trace ↗</button>
          </div>
        </summary>
        <div class="aux-pill-body">
          <div class="tool-io-block">
            <span class="tool-io-label">Tool Output (${escapeHtml(tc.status || "DONE")})</span>
            <div class="tool-io-pre">${escapeHtml(outText)}</div>
          </div>
          <div class="tool-io-block">
            <span class="tool-io-label">Input Arguments</span>
            <div class="tool-io-pre">${escapeHtml(tc.argsPreview || "{}")}</div>
          </div>
        </div>
      </details>
    `;
  }

  function buildMessageHtml(item, compact = false) {
    if (item.role === "user") {
      return `
        <div class="msg-user">
          <div class="msg-body-text">${renderMarkdownLite(item.content)}</div>
        </div>
      `;
    }

    const timeLabel = fmtTimeShort(item.updatedAt || item.createdAt);
    const stepIdx = item.stepIndex || 0;
    const tcalls = item.toolCalls || [];
    const hasContent = Boolean(item.content && item.content.trim());
    let auxHtml = "";

    // 1. Reasoning trace (shown in Full Chat, or in Overview when turn is in-progress)
    if (item.thinking && (!compact || !hasContent)) {
      auxHtml += `
        <details class="aux-pill" data-detail-id="thought-${stepIdx}">
          <summary>
            <div class="aux-pill-summary-left">
              <span class="aux-type-tag">thought</span>
              <span class="tool-target-text">${escapeHtml(
                item.thinkingSummary || "Reasoning trace"
              )}</span>
            </div>
          </summary>
          <div class="aux-pill-body">${escapeHtml(item.thinking)}</div>
        </details>
      `;
    }

    // 2. Tool calls + outputs (grouped cleanly so 50 tools never bury the message text)
    if (tcalls.length > 0) {
      const lastTc = tcalls[tcalls.length - 1];
      const lastSummary = `${lastTc.name}${
        lastTc.target ? ` · ${lastTc.target}` : lastTc.action ? ` · ${lastTc.action}` : ""
      }`;
      const errCount = tcalls.filter((t) => t.status === "ERROR").length;
      const runCount = tcalls.filter((t) => t.status === "RUNNING").length;
      const dotClass = runCount > 0 ? "running" : errCount > 0 ? "error" : "";

      if (hasContent || tcalls.length > 3) {
        if (!hasContent && tcalls.length > 3) {
          // Turn is actively running: collapse earlier tools and show latest 3 live below
          const earlier = tcalls.slice(0, -3);
          const recent = tcalls.slice(-3);
          auxHtml += `
            <details class="tools-group-drawer" data-detail-id="tg-early-${stepIdx}">
              <summary>
                <div class="tools-group-summary-left">
                  <span class="aux-type-tag">+${earlier.length} earlier tools</span>
                  <span class="tools-group-summary-latest">Click to inspect earlier tool outputs</span>
                </div>
                <span class="kpi-sub">Show ▾</span>
              </summary>
              <div class="tools-group-body">
                ${earlier.map((tc) => buildSingleToolDetailHtml(tc, stepIdx)).join("")}
              </div>
            </details>
            ${recent.map((tc) => buildSingleToolDetailHtml(tc, stepIdx)).join("")}
          `;
        } else {
          // Turn has content (or completed): group all tool calls into one clean expandable drawer
          auxHtml += `
            <details class="tools-group-drawer" data-detail-id="tg-all-${stepIdx}">
              <summary title="Click to expand all ${tcalls.length} tool calls and their outputs">
                <div class="tools-group-summary-left">
                  <span class="status-dot-sm ${dotClass}"></span>
                  <span class="aux-type-tag">${tcalls.length} tool${
            tcalls.length > 1 ? "s" : ""
          } executed</span>
                  <span class="tools-group-summary-latest">Last: ${escapeHtml(lastSummary)}</span>
                </div>
                <span class="kpi-sub">Outputs ▾</span>
              </summary>
              <div class="tools-group-body">
                ${tcalls.map((tc) => buildSingleToolDetailHtml(tc, stepIdx)).join("")}
              </div>
            </details>
          `;
        }
      } else {
        // <= 3 tools while turn is in-progress: show them directly
        auxHtml += tcalls.map((tc) => buildSingleToolDetailHtml(tc, stepIdx)).join("");
      }
    }

    // 3. Primary body: either the assistant's markdown response OR a Live Progress banner while tools run
    let bodyHtml = "";
    if (hasContent) {
      bodyHtml = `<div class="msg-body-text">${renderMarkdownLite(item.content)}</div>`;
    } else {
      const liveAction =
        item.latestAction ||
        (tcalls.length
          ? `${tcalls[tcalls.length - 1].name} · ${
              tcalls[tcalls.length - 1].target || tcalls[tcalls.length - 1].action || ""
            }`
          : "Processing request...");
      const liveThought = item.thinkingSummary || "";
      bodyHtml = `
        <div class="msg-live-progress">
          <div class="msg-live-progress-head">
            <span class="status-dot-sm running"></span>
            <span>Agent working: ${escapeHtml(liveAction)}</span>
          </div>
          ${
            liveThought && liveThought !== liveAction
              ? `<div class="msg-live-progress-thought">${escapeHtml(liveThought)}</div>`
              : ""
          }
        </div>
      `;
    }

    return `
      <div class="msg-agent">
        <div class="msg-header">
          <span class="msg-role"><span class="msg-role-dot"></span>Jetski Agent</span>
          <span>${escapeHtml(timeLabel)}</span>
        </div>
        ${bodyHtml}
        ${auxHtml ? `<div class="msg-aux-list">${auxHtml}</div>` : ""}
      </div>
    `;
  }

  function updateChatContainerPreservingDetails(container, html, shouldScrollBottom) {
    if (!container) return;
    const openIds = new Set();
    container.querySelectorAll("details[data-detail-id]").forEach((d) => {
      if (d.open) {
        openIds.add(d.getAttribute("data-detail-id"));
      }
    });

    container.innerHTML = html;

    if (openIds.size > 0) {
      container.querySelectorAll("details[data-detail-id]").forEach((d) => {
        if (openIds.has(d.getAttribute("data-detail-id"))) {
          d.open = true;
        }
      });
    }

    if (shouldScrollBottom) {
      container.scrollTop = container.scrollHeight;
    }
  }

  function renderChatAndOverview(data, forceScrollBottom = false) {
    const chat = data.chat || {};
    const convs = (data.conversations && data.conversations.list) || [];
    const activeConv =
      convs.find((c) => c.id === state.activeConvId) || convs[0] || null;
    const isRunning = chat.status === "RUNNING";
    const items = chat.items || [];
    const subagents = chat.subagents || [];

    // Update live subagent mini-feed strips
    renderSubagentStrips(subagents);

    // --- Full Chat Header ---
    const badge = document.getElementById("chat-status-badge");
    const metaEl = document.getElementById("chat-session-meta");
    const stopBtn = document.getElementById("btn-stop-agent");
    const openNativeBtn = document.getElementById("btn-open-native");

    if (badge) {
      badge.textContent = chat.status || "IDLE";
      badge.classList.toggle("running", isRunning);
    }
    if (metaEl) {
      const subCount = chat.subagentCount || 0;
      metaEl.textContent = `${chat.totalSteps || items.length || 0} steps${
        subCount > 0 ? ` · ${subCount} subagents` : ""
      }`;
    }
    if (stopBtn) {
      stopBtn.hidden = !isRunning;
    }

    if (openNativeBtn) {
      const hostConvId =
        (data.conversations && data.conversations.hostActiveId) || "";
      if (state.activeConvId && hostConvId && state.activeConvId !== hostConvId) {
        openNativeBtn.hidden = false;
      } else {
        openNativeBtn.hidden = true;
      }
    }

    // --- Overview Bento Quadrant 1 (Chat) Header ---
    const bentoTitle = document.getElementById("bento-chat-title");
    const bentoSub = document.getElementById("bento-chat-subtitle");
    const bentoStatus = document.getElementById("bento-chat-status");
    if (bentoTitle) {
      bentoTitle.textContent = activeConv ? activeConv.title : "Agent Chat";
      bentoTitle.title = activeConv ? activeConv.title : "Agent Chat";
    }
    if (bentoSub) {
      const subCount = chat.subagentCount || 0;
      bentoSub.textContent = `${convs.length} sessions · ${
        chat.totalSteps || items.length || 0
      } steps${subCount > 0 ? ` · ${subCount} subagents` : ""}`;
    }
    if (bentoStatus) {
      bentoStatus.textContent = chat.status || "IDLE";
      bentoStatus.classList.toggle("running", isRunning);
    }

    // --- Overview Bento Quadrant 2 (Agent Tracer) Subtitle ---
    const bentoTracerSub = document.getElementById("bento-tracer-sub");
    if (bentoTracerSub) {
      const subCount = chat.subagentCount || 0;
      bentoTracerSub.textContent =
        subCount > 0
          ? `${subCount} subagent${subCount > 1 ? "s" : ""} spawned · Click Trace ↗ on any tool to focus`
          : "Live DAG & Architecture · Click Trace ↗ on any tool to focus";
    }

    // Build a fine-grained signature of recent messages, content length, tool counts, and tool outputs
    // so the UI ALWAYS re-renders when new tool calls, tool outputs, or final responses arrive!
    const tailSig = items
      .slice(-3)
      .map((it) => {
        const tcs = it.toolCalls || [];
        const lastTc = tcs.length ? tcs[tcs.length - 1] : {};
        return [
          it.stepIndex,
          it.lastStepIndex || it.stepIndex,
          it.status || "",
          (it.content || "").length,
          (it.thinking || "").length,
          tcs.length,
          lastTc.status || "",
          (lastTc.outputPreview || "").length,
        ].join(":");
      })
      .join("|");
    const hash = `${state.activeConvId}:${items.length}:${chat.totalSteps || 0}:${chat.status}:${tailSig}`;
    if (!forceScrollBottom && hash === state.lastChatHash) {
      return;
    }
    state.lastChatHash = hash;

    // 1. Full Chat Container
    const container = document.getElementById("chat-messages");
    if (container) {
      const wasNearBottom =
        container.scrollHeight - container.scrollTop - container.clientHeight < 120;
      if (items.length === 0) {
        container.innerHTML = `<div class="empty-state">No messages found for this conversation yet. Send a prompt below to begin!</div>`;
      } else {
        const html = items.map((it) => buildMessageHtml(it, false)).join("");
        updateChatContainerPreservingDetails(container, html, forceScrollBottom || wasNearBottom);
      }
    }

    // 2. Overview Bento Chat Container (last 10 turns for crisp readability)
    const bentoContainer = document.getElementById("bento-chat-messages");
    if (bentoContainer) {
      const wasNearBottom =
        bentoContainer.scrollHeight -
          bentoContainer.scrollTop -
          bentoContainer.clientHeight <
        120;
      if (items.length === 0) {
        bentoContainer.innerHTML = `<div class="empty-state-sm">No messages in this session yet. Type below to prompt Jetski!</div>`;
      } else {
        const recentItems = items.slice(-10);
        const html = recentItems.map((it) => buildMessageHtml(it, true)).join("");
        updateChatContainerPreservingDetails(
          bentoContainer,
          html,
          forceScrollBottom || wasNearBottom
        );
      }
    }
  }

  function renderAutomationsAndOverview(data) {
    const autos = (data.automations && data.automations.items) || [];
    const sidecars = (data.sidecars && data.sidecars.items) || [];
    const mcps = (data.mcp && data.mcp.servers) || [];

    // --- Overview Quadrant 4 (Automations & Runtime) ---
    const bentoAutoSub = document.getElementById("bento-auto-sub");
    if (bentoAutoSub) {
      bentoAutoSub.textContent = `${autos.length} cron jobs · ${sidecars.length} sidecars · Click any row to explain`;
    }

    const bentoAutoList = document.getElementById("bento-auto-list");
    if (bentoAutoList) {
      const combinedRows = [];
      autos.forEach((a) => {
        const autoId = a.id || a.plugin;
        const isActive = a.status === "ACTIVE";
        const isPaused = a.status === "PAUSED";
        const shortSchedule = compactCronLabel(a.cron);
        const info = buildAutomationExplanation(a);
        const tooltip = info
          ? `${info.name} (${info.status})\n${info.purpose}\nSchedule: ${info.scheduleSgt}\n(Click to pin/hide full explanation)`
          : `${a.name} — ${a.cron}`;
        combinedRows.push(`
          <div class="bento-auto-row ${
            state.selectedAutomation === autoId ? "active-auto" : ""
          }" data-auto-id="${escapeHtml(autoId)}" title="${escapeHtml(tooltip)}">
            <div class="bento-auto-row-left">
              <span class="status-dot-sm ${
                isActive ? "running" : isPaused ? "paused" : ""
              }"></span>
              <span class="bento-auto-name">${escapeHtml(a.name)}</span>
              <span class="metric-help-badge">?</span>
            </div>
            <span class="bento-auto-meta">${escapeHtml(shortSchedule)}</span>
          </div>
        `);
      });

      sidecars.forEach((s) => {
        const sideId = s.id || `${s.plugin}/${s.sidecar}`;
        const isRun = s.status === "RUNNING";
        const info = buildAutomationExplanation(s);
        const tooltip = info
          ? `${info.name} (${info.status})\n${info.purpose}\n(Click to pin/hide full explanation)`
          : `${s.title} (${s.status})`;
        combinedRows.push(`
          <div class="bento-auto-row ${
            state.selectedAutomation === sideId ? "active-auto" : ""
          }" data-auto-id="${escapeHtml(sideId)}" title="${escapeHtml(tooltip)}">
            <div class="bento-auto-row-left">
              <span class="status-dot-sm ${isRun ? "running" : ""}"></span>
              <span class="bento-auto-name">${escapeHtml(s.title)}</span>
              <span class="metric-help-badge">?</span>
            </div>
            <span class="bento-auto-meta">${
              s.pid ? `PID ${s.pid}` : escapeHtml(s.type)
            }</span>
          </div>
        `);
      });

      bentoAutoList.innerHTML =
        combinedRows.slice(0, 6).join("") ||
        `<div class="empty-state-sm">No automations or sidecars found.</div>`;
    }

    const bentoMcpStrip = document.getElementById("bento-mcp-strip");
    if (bentoMcpStrip) {
      bentoMcpStrip.innerHTML = mcps
        .map(
          (m) =>
            `<span class="mcp-chip" title="${m.lazyCount} tools available">${escapeHtml(
              m.displayName
            )} (${m.lazyCount})</span>`
        )
        .join("");
    }

    // --- Full Automations Tab ---
    const autoGrid = document.getElementById("automations-grid");
    if (autoGrid) {
      if (autos.length === 0) {
        autoGrid.innerHTML = `<div class="empty-state">No scheduled cron automations configured.</div>`;
      } else {
        autoGrid.innerHTML = autos
          .map((a) => {
            const autoId = a.id || a.plugin;
            const isActive = a.status === "ACTIVE";
            const isPaused = a.status === "PAUSED";
            const pauseLabel = a.restartPolicy === "never" ? "Resume" : "Pause";
            return `
              <div class="item-card ${
                state.selectedAutomation === autoId ? "active-auto" : ""
              }" data-auto-id="${escapeHtml(autoId)}">
                <div class="item-card-top">
                  <div class="item-card-title">${escapeHtml(a.name)}</div>
                  <span class="status-badge ${
                    isActive ? "running" : isPaused ? "paused" : ""
                  }">${escapeHtml(a.status)}</span>
                </div>
                <div class="item-card-desc">${escapeHtml(a.description)}</div>
                <div class="item-card-meta">
                  <span>Schedule: <code>${escapeHtml(a.cron)}</code></span>
                  ${a.pid ? `<span>· PID ${a.pid}</span>` : ""}
                  ${a.extraBadge ? `<span>· ${escapeHtml(a.extraBadge)}</span>` : ""}
                </div>
                <div class="item-card-footer">
                  <span class="kpi-sub">Target: ${escapeHtml(
                    a.targetSummary || a.lastFired || "Scheduled"
                  )}</span>
                  <div class="item-card-btn-group">
                    <button type="button" class="btn-ghost btn-sm js-explain-auto" data-auto-id="${escapeHtml(
                      autoId
                    )}" title="Explain what this automation does">? Explain</button>
                    ${
                      a.canTogglePause
                        ? `<button type="button" class="btn-secondary btn-sm js-toggle-auto" data-plugin="${escapeHtml(
                            a.plugin
                          )}" title="Toggle restart_policy in sidecar.json">${pauseLabel}</button>`
                        : ""
                    }
                    ${
                      a.canTriggerNow
                        ? `<button type="button" class="btn-secondary btn-sm js-trigger-auto" data-plugin="${escapeHtml(
                            a.plugin
                          )}">Run Now</button>`
                        : ""
                    }
                  </div>
                </div>
              </div>
            `;
          })
          .join("");
      }
    }

    const sideGrid = document.getElementById("sidecars-grid");
    if (sideGrid) {
      if (sidecars.length === 0) {
        sideGrid.innerHTML = `<div class="empty-state">No sidecar plugins installed.</div>`;
      } else {
        sideGrid.innerHTML = sidecars
          .map((s) => {
            const sideId = s.id || `${s.plugin}/${s.sidecar}`;
            const isRun = s.status === "RUNNING";
            return `
              <div class="item-card ${
                state.selectedAutomation === sideId ? "active-auto" : ""
              }" data-auto-id="${escapeHtml(sideId)}">
                <div class="item-card-top">
                  <div class="item-card-title">${escapeHtml(s.title)}</div>
                  <span class="status-badge ${isRun ? "running" : ""}">${escapeHtml(
              s.status
            )}</span>
                </div>
                <div class="item-card-desc">${escapeHtml(
                  s.description || `${s.plugin}/${s.sidecar}`
                )}</div>
                <div class="item-card-meta">
                  <span>Type: <code>${escapeHtml(s.type)}</code></span>
                  ${s.pid ? `<span>· PID ${s.pid}</span>` : ""}
                  ${s.uptimeFormatted ? `<span>· Uptime ${escapeHtml(s.uptimeFormatted)}</span>` : ""}
                </div>
                <div class="item-card-footer">
                  <span class="kpi-sub">ID: <code>${escapeHtml(sideId)}</code></span>
                  <button type="button" class="btn-ghost btn-sm js-explain-auto" data-auto-id="${escapeHtml(
                    sideId
                  )}">? Explain</button>
                </div>
              </div>
            `;
          })
          .join("");
      }
    }

    renderAutomationExplainers();
  }

  function renderTokensAndOverview(data) {
    const tok = data.tokens || {};
    const active = tok.activeSession || {};
    const global = tok.globalRecent || {};
    const turns = active.turns || [];

    // Context Window Saturation calculation (~200k compaction threshold)
    const ctxWin = active.contextWindowTokens || 0;
    const ctxSatPct = Math.min(100, Math.round((ctxWin / 200_000) * 100));
    const ctxText = `${fmtTokens(ctxWin)} / 200k (${ctxSatPct}%)`;
    const satClass = ctxSatPct >= 85 ? "danger" : ctxSatPct >= 60 ? "warn" : "";

    ["bento", "tokens"].forEach((prefix) => {
      const valEl = document.getElementById(`${prefix}-context-val`);
      const fillEl = document.getElementById(`${prefix}-context-fill`);
      if (valEl) valEl.textContent = ctxText;
      if (fillEl) {
        fillEl.style.width = `${ctxSatPct}%`;
        fillEl.classList.remove("warn", "danger");
        if (satClass) fillEl.classList.add(satClass);
      }
    });

    // --- Overview Quadrant 3 (Tokens & Cache) ---
    const bentoModel = document.getElementById("bento-model-label");
    if (bentoModel) {
      bentoModel.textContent = `Model: ${active.model || "Gemini Next"}`;
    }
    const bentoSession = document.getElementById("bento-tok-session");
    if (bentoSession) {
      bentoSession.textContent = fmtTokens(active.totalTokens || 0);
    }
    const bentoCache = document.getElementById("bento-tok-cache");
    if (bentoCache) {
      bentoCache.textContent = `${active.cacheHitPct || 0}%`;
    }
    const bentoCost = document.getElementById("bento-tok-cost");
    if (bentoCost) {
      bentoCost.textContent = `$${(active.estCostUsd || 0).toFixed(2)}`;
    }
    const bentoGlobal = document.getElementById("bento-tok-global");
    if (bentoGlobal) {
      bentoGlobal.textContent = fmtTokens(global.totalTokens || 0);
    }
    const bentoTurnCount = document.getElementById("bento-turn-count");
    if (bentoTurnCount) {
      bentoTurnCount.textContent = `${active.turnCount || turns.length} turns`;
    }

    const bentoBars = document.getElementById("bento-turn-bars");
    if (bentoBars) {
      if (turns.length === 0) {
        bentoBars.innerHTML = `<div class="empty-state-sm">No LLM turns recorded for this session yet.</div>`;
      } else {
        const recentTurns = turns.slice(-4);
        const maxTok = Math.max(
          1,
          ...recentTurns.map((t) => (t.inputTokens || 0) + (t.outputTokens || 0))
        );
        bentoBars.innerHTML = recentTurns
          .map((t) => {
            const total = (t.inputTokens || 0) + (t.outputTokens || 0);
            const cached = Math.min(t.cacheReadTokens || 0, t.inputTokens || 0);
            const fresh = Math.max(0, (t.inputTokens || 0) - cached);
            const out = t.outputTokens || 0;

            const cachedPct = ((cached / maxTok) * 100).toFixed(1);
            const freshPct = ((fresh / maxTok) * 100).toFixed(1);
            const outPct = ((out / maxTok) * 100).toFixed(1);

            return `
              <div class="turn-bar-row">
                <span class="kpi-sub">Turn #${t.turn}</span>
                <div class="turn-bar-track" title="Turn #${t.turn} — Cached Context: ${fmtTokens(
                  cached
                )} | Fresh Input: ${fmtTokens(fresh)} | Output: ${fmtTokens(out)}">
                  <div class="turn-bar-cached" style="width:${cachedPct}%"></div>
                  <div class="turn-bar-fresh" style="width:${freshPct}%"></div>
                  <div class="turn-bar-out" style="width:${outPct}%"></div>
                </div>
                <span class="kpi-sub" style="text-align:right">${fmtTokens(
                  total
                )} (${fmtTokens(out)} out)</span>
              </div>
            `;
          })
          .join("");
      }
    }

    // --- Full Tokens Tab ---
    const elSession = document.getElementById("kpi-session-tokens");
    const elSessionSub = document.getElementById("kpi-session-sub");
    const elCache = document.getElementById("kpi-cache-pct");
    const elCacheSub = document.getElementById("kpi-cache-sub");
    const elCost = document.getElementById("kpi-cost");
    const elModel = document.getElementById("kpi-model-name");
    const elGlobal = document.getElementById("kpi-global-tokens");
    const elGlobalSub = document.getElementById("kpi-global-sub");

    if (elSession) elSession.textContent = fmtTokens(active.totalTokens || 0);
    if (elSessionSub) {
      elSessionSub.textContent = `${fmtTokens(
        active.inputTokens || 0
      )} in · ${fmtTokens(active.outputTokens || 0)} out (${
        active.turnCount || 0
      } turns)`;
    }
    if (elCache) elCache.textContent = `${active.cacheHitPct || 0}%`;
    if (elCacheSub) {
      elCacheSub.textContent = `${fmtTokens(
        active.cacheReadTokens || 0
      )} cached tokens reused`;
    }
    if (elCost) elCost.textContent = `$${(active.estCostUsd || 0).toFixed(2)}`;
    if (elModel) elModel.textContent = `Model: ${active.model || "Gemini Next"}`;
    if (elGlobal) elGlobal.textContent = fmtTokens(global.totalTokens || 0);
    if (elGlobalSub) {
      elGlobalSub.textContent = `Across ${
        global.sessionCount || 0
      } sessions · Est. $${(global.estCostUsd || 0).toFixed(2)}`;
    }

    const barsBox = document.getElementById("turn-bars-container");
    if (barsBox) {
      if (turns.length === 0) {
        barsBox.innerHTML = `<div class="empty-state">No LLM turn telemetry recorded for this conversation yet.</div>`;
      } else {
        const maxTok = Math.max(
          1,
          ...turns.map((t) => (t.inputTokens || 0) + (t.outputTokens || 0))
        );
        barsBox.innerHTML = turns
          .map((t) => {
            const total = (t.inputTokens || 0) + (t.outputTokens || 0);
            const cached = Math.min(t.cacheReadTokens || 0, t.inputTokens || 0);
            const fresh = Math.max(0, (t.inputTokens || 0) - cached);
            const out = t.outputTokens || 0;

            const cachedPct = ((cached / maxTok) * 100).toFixed(1);
            const freshPct = ((fresh / maxTok) * 100).toFixed(1);
            const outPct = ((out / maxTok) * 100).toFixed(1);

            return `
              <div class="turn-bar-row">
                <span class="kpi-sub">Turn #${t.turn}</span>
                <div class="turn-bar-track" title="Turn #${t.turn} — Cached: ${fmtTokens(
                  cached
                )} | Fresh Input: ${fmtTokens(fresh)} | Output: ${fmtTokens(out)}">
                  <div class="turn-bar-cached" style="width:${cachedPct}%"></div>
                  <div class="turn-bar-fresh" style="width:${freshPct}%"></div>
                  <div class="turn-bar-out" style="width:${outPct}%"></div>
                </div>
                <span class="kpi-sub" style="text-align:right">${fmtTokens(
                  total
                )} (${fmtTokens(out)} out)</span>
              </div>
            `;
          })
          .join("");
      }
    }

    // MCP & Memories (Clean, emoji-free rows)
    const mcpServers = (data.mcp && data.mcp.servers) || [];
    const mcpBadge = document.getElementById("mcp-count-badge");
    if (mcpBadge) mcpBadge.textContent = String(mcpServers.length);

    const mcpList = document.getElementById("mcp-servers-list");
    if (mcpList) {
      mcpList.innerHTML =
        mcpServers
          .map(
            (s) => `
          <div class="simple-row">
            <strong>${escapeHtml(s.displayName)}</strong>
            <span class="badge-subtle">${s.lazyCount} tools</span>
          </div>
        `
          )
          .join("") || `<div class="empty-state">No MCP servers found.</div>`;
    }

    const mems = (data.memories && data.memories.items) || [];
    const memList = document.getElementById("memories-list");
    if (memList) {
      memList.innerHTML =
        mems
          .map(
            (m) => `
          <div class="simple-row">
            <span><code>${escapeHtml(m.path)}</code></span>
            <span class="kpi-sub">${escapeHtml(m.updatedAt)}</span>
          </div>
        `
          )
          .join("") || `<div class="empty-state">No memory files found.</div>`;
    }

    renderMetricExplainers(data);
  }

  // =========================================================================
  // 9. State Polling & Prompt Dispatch
  // =========================================================================
  async function fetchState(forceScrollBottom = false) {
    if (!forceScrollBottom && document.hidden) return;
    const seq = ++state.fetchSeq;
    try {
      const qs = state.activeConvId
        ? `?convId=${encodeURIComponent(state.activeConvId)}`
        : "";
      const res = await apiFetch(`api/state${qs}`);
      if (!res.ok) return;
      const data = await res.json();

      if (seq !== state.fetchSeq) return;

      state.data = data;

      renderTopbarAndPicker(data);
      renderChatAndOverview(data, forceScrollBottom);
      renderAutomationsAndOverview(data);
      renderTokensAndOverview(data);
    } catch (err) {
      console.warn("Jetski Harness state poll error:", err);
    }
  }

  async function dispatchPrompt(promptText, forceNewConv = false, inputElement = null, btnElement = null) {
    const prompt = (promptText || "").trim();
    if (!prompt || state.sendingPrompt) return;

    const errEl = document.getElementById("composer-error");
    if (errEl) errEl.hidden = true;

    state.sendingPrompt = true;
    const origLabel = btnElement ? btnElement.textContent : "Send ↵";
    if (btnElement) {
      btnElement.disabled = true;
      btnElement.textContent = "Sending...";
    }

    try {
      const targetConvId = forceNewConv ? "" : state.activeConvId;
      const res = await apiFetch("api/chat/send", {
        method: "POST",
        body: JSON.stringify({
          prompt,
          convId: targetConvId,
        }),
      });
      const r = await res.json();
      if (!r.ok) {
        if (errEl) {
          errEl.textContent = `Could not send prompt: ${r.error || "Unknown error"}`;
          errEl.hidden = false;
        }
      } else {
        if (inputElement) inputElement.value = "";
        const newChk = document.getElementById("chk-new-conversation");
        if (newChk) newChk.checked = false;
        if (r.conversationId) {
          state.activeConvId = r.conversationId;
        }
        await fetchState(true);
      }
    } catch (e) {
      if (errEl) {
        errEl.textContent = `Network error sending prompt: ${e.message}`;
        errEl.hidden = false;
      }
    } finally {
      state.sendingPrompt = false;
      if (btnElement) {
        btnElement.disabled = false;
        btnElement.textContent = origLabel;
      }
    }
  }

  function openFullScreenTab() {
    window.open(window.location.href, "_blank", "noopener,noreferrer");
  }

  function initDelegatedDynamicListeners() {
    document.addEventListener("click", async (e) => {
      const target = e.target;
      if (!target || typeof target.closest !== "function") return;

      // 1. Focus tool or subagent step in embedded Agent Tracer
      const focusBtn = target.closest(".js-focus-step");
      if (focusBtn) {
        e.stopPropagation();
        e.preventDefault();
        focusStepInTracer(
          focusBtn.getAttribute("data-step-index"),
          focusBtn.getAttribute("data-tool-name")
        );
        return;
      }

      // 2. Close metric explainer drawer
      if (target.closest(".js-close-explainer")) {
        e.stopPropagation();
        state.selectedMetric = "";
        if (state.data) renderMetricExplainers(state.data);
        return;
      }

      // 3. Close automation explainer drawer
      if (target.closest(".js-close-auto-explainer")) {
        e.stopPropagation();
        state.selectedAutomation = "";
        renderAutomationExplainers();
        return;
      }

      // 4. Pause / Resume automation button
      const toggleBtn = target.closest(".js-toggle-auto");
      if (toggleBtn) {
        e.stopPropagation();
        const plugin = toggleBtn.getAttribute("data-plugin");
        toggleBtn.disabled = true;
        try {
          await apiFetch("api/automation/toggle", {
            method: "POST",
            body: JSON.stringify({ plugin }),
          });
        } catch (_) {}
        toggleBtn.disabled = false;
        fetchState(false);
        return;
      }

      // 5. Trigger automation now button
      const triggerBtn = target.closest(".js-trigger-auto");
      if (triggerBtn) {
        e.stopPropagation();
        const plugin = triggerBtn.getAttribute("data-plugin");
        triggerBtn.disabled = true;
        triggerBtn.textContent = "Triggering...";
        try {
          const res = await apiFetch("api/automation/trigger", {
            method: "POST",
            body: JSON.stringify({ plugin }),
          });
          const r = await res.json();
          triggerBtn.textContent = r.ok ? "Triggered" : "Failed";
        } catch (_) {
          triggerBtn.textContent = "Error";
        }
        setTimeout(() => {
          triggerBtn.disabled = false;
          triggerBtn.textContent = "Run Now";
          fetchState(false);
        }, 2000);
        return;
      }

      // 6. Explain automation button or Overview automation row click
      const explainEl = target.closest(".js-explain-auto, .bento-auto-row[data-auto-id]");
      if (explainEl) {
        e.stopPropagation();
        const id = explainEl.getAttribute("data-auto-id") || "";
        state.selectedAutomation = state.selectedAutomation === id ? "" : id;
        renderAutomationExplainers();
      }
    });
  }

  function initControls() {
    initDelegatedDynamicListeners();

    // Theme toggle
    const themeBtn = document.getElementById("btn-theme-toggle");
    if (themeBtn) {
      themeBtn.addEventListener("click", () => {
        applyTheme(state.theme === "dark" ? "light" : "dark", true);
      });
    }

    // All Full Screen buttons (Topbar + Chat Header)
    document.querySelectorAll(".js-btn-fullscreen").forEach((btn) => {
      btn.addEventListener("click", openFullScreenTab);
    });

    // Overview Agent Tracer Timeline | Architecture switcher
    document.querySelectorAll(".js-tracer-mode").forEach((btn) => {
      btn.addEventListener("click", () => {
        setOverviewTracerMode(btn.getAttribute("data-mode"));
      });
    });

    // Refresh button
    const refreshBtn = document.getElementById("btn-refresh");
    if (refreshBtn) {
      refreshBtn.addEventListener("click", () => fetchState(false));
    }

    // All Conversation Selectors (Topbar + Full Chat Header)
    document.querySelectorAll(".js-session-select").forEach((sel) => {
      sel.addEventListener("change", () => {
        if (sel.value) {
          selectConversation(sel.value);
        }
      });
    });

    // Interactive Metric Explainer Cards (Click to toggle explanation)
    document.querySelectorAll(".js-metric-card").forEach((card) => {
      card.addEventListener("click", () => {
        const key = card.getAttribute("data-metric") || "";
        state.selectedMetric = state.selectedMetric === key ? "" : key;
        if (state.data) {
          renderMetricExplainers(state.data);
        }
      });
    });

    // Prev / Next Conversation Arrows
    const prevBtn = document.getElementById("btn-prev-session");
    if (prevBtn) {
      prevBtn.addEventListener("click", () => cycleConversation(-1));
    }
    const nextBtn = document.getElementById("btn-next-session");
    if (nextBtn) {
      nextBtn.addEventListener("click", () => cycleConversation(1));
    }

    // New session button
    const newBtn = document.getElementById("btn-new-session");
    if (newBtn) {
      newBtn.addEventListener("click", () => {
        switchTab("chat");
        const chk = document.getElementById("chk-new-conversation");
        if (chk) chk.checked = true;
        const input = document.getElementById("prompt-input");
        if (input) {
          input.focus();
          input.placeholder = "Type your prompt to start a brand new conversation...";
        }
      });
    }

    // Full Chat Composer
    const sendBtn = document.getElementById("btn-send-prompt");
    const inputEl = document.getElementById("prompt-input");
    const newChk = document.getElementById("chk-new-conversation");
    if (sendBtn && inputEl) {
      sendBtn.addEventListener("click", () => {
        dispatchPrompt(inputEl.value, !!(newChk && newChk.checked), inputEl, sendBtn);
      });
      inputEl.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          dispatchPrompt(inputEl.value, !!(newChk && newChk.checked), inputEl, sendBtn);
        }
      });
    }

    // Overview Quick Composer
    const bentoSendBtn = document.getElementById("bento-btn-send");
    const bentoInputEl = document.getElementById("bento-prompt-input");
    if (bentoSendBtn && bentoInputEl) {
      bentoSendBtn.addEventListener("click", () => {
        dispatchPrompt(bentoInputEl.value, false, bentoInputEl, bentoSendBtn);
      });
      bentoInputEl.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          dispatchPrompt(bentoInputEl.value, false, bentoInputEl, bentoSendBtn);
        }
      });
    }

    // Stop active agent
    const stopBtn = document.getElementById("btn-stop-agent");
    if (stopBtn) {
      stopBtn.addEventListener("click", async () => {
        if (!state.activeConvId) return;
        stopBtn.disabled = true;
        await apiFetch("api/chat/stop", {
          method: "POST",
          body: JSON.stringify({ convId: state.activeConvId }),
        });
        stopBtn.disabled = false;
        fetchState(false);
      });
    }

    // Load selected conversation into Jetski's Left Pane
    const openNativeBtn = document.getElementById("btn-open-native");
    if (openNativeBtn) {
      openNativeBtn.addEventListener("click", () => {
        if (!state.activeConvId) return;
        if (
          window.sidecar &&
          window.sidecar.ui &&
          typeof window.sidecar.ui.toggleConversation === "function"
        ) {
          window.sidecar.ui.toggleConversation(state.activeConvId);
        } else {
          window.parent.postMessage(
            {
              type: "toggle-conversation",
              payload: { conversationId: state.activeConvId },
            },
            "*"
          );
        }
      });
    }

    initBentoGridCustomization();
    initTracerIframes();
  }

  // Boot
  document.addEventListener("DOMContentLoaded", () => {
    applyTheme(state.theme, false);
    initTabs();
    initControls();
    fetchState(true);
    setInterval(() => fetchState(false), 4000);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) fetchState(false);
    });
  });
})();
