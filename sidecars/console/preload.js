/**
 * @fileoverview Frontend bridge for Antigravity sidecars.
 */
(function() {
  const config = (function() {
    const params = new URLSearchParams(window.location.search);
    const token = params.get('token');
    const conversationId = params.get('conversationId');
    return { token, conversationId };
  })();

  const { token, conversationId } = config;

  let currentWorkspaceUris = [];
  let hasInitialWorkspace = false;
  const workspaceListeners = new Set();
  let resolveInitialWorkspace;
  const initialWorkspacePromise = new Promise((resolve) => {
    resolveInitialWorkspace = resolve;
  });

  function areUriArraysEqual(a, b) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  function notifyWorkspaceListener(listener) {
    try {
      listener([...currentWorkspaceUris]);
    } catch (err) {
      console.error('workspace-change listener failed:', err);
    }
  }

  window.addEventListener('message', (event) => {
    if (event.data?.type === 'theme-change') {
      const vars = event.data.payload.cssVariables;
      for (const [key, value] of Object.entries(vars)) {
        document.documentElement.style.setProperty(key, value);
      }
    } else if (event.data?.type === 'workspace-change') {
      const payload = event.data.payload ?? {};
      const nextWorkspaceUris = Array.isArray(payload.workspaceUris)
        ? [...payload.workspaceUris]
        : [];
      const isFirstPush = !hasInitialWorkspace;
      const hasChanged =
        isFirstPush ||
        !areUriArraysEqual(currentWorkspaceUris, nextWorkspaceUris);
      currentWorkspaceUris = nextWorkspaceUris;
      hasInitialWorkspace = true;
      resolveInitialWorkspace();
      if (hasChanged) {
        for (const listener of workspaceListeners) {
          notifyWorkspaceListener(listener);
        }
      }
    }
  });

  function secureFetch(url, options = {}) {
    const headers = new Headers(options.headers || {});
    headers.set('Content-Type', 'application/json');
    if (token) {
      headers.set('X-Sidecar-Token', token);
    }
    return fetch(url, { ...options, headers });
  }

  async function getWorkspaceUris() {
    if (window.parent === window) {
      return [];
    }
    await initialWorkspacePromise;
    return [...currentWorkspaceUris];
  }

  window.sidecar = {
    conversationId,
    fetch: secureFetch,
    getWorkspaceUris,
    onWorkspaceChange(listener) {
      if (typeof listener !== 'function') {
        return () => {};
      }
      workspaceListeners.add(listener);
      if (hasInitialWorkspace) {
        queueMicrotask(() => {
          if (workspaceListeners.has(listener)) {
            notifyWorkspaceListener(listener);
          }
        });
      }
      return () => {
        workspaceListeners.delete(listener);
      };
    },
    agent: {
      async sendMessage(message, convId) {
        const res = await secureFetch('/_sidecar/send-message', {
          method: 'POST',
          body: JSON.stringify({
            conversationId: convId || conversationId,
            message
          })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      },
      async startConversation(message, title) {
        const res = await secureFetch('/_sidecar/new-conversation', {
          method: 'POST',
          body: JSON.stringify({ message, title })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      },
      async getConversationMetadata(convId) {
        const res = await secureFetch('/_sidecar/get-conversation-metadata', {
          method: 'POST',
          body: JSON.stringify({ conversationId: convId || conversationId })
        });
        if (!res.ok) throw new Error(await res.text());
        return res.json();
      }
    },
    ui: {
      toggleAuxPane(request) {
        window.parent.postMessage({
          type: 'toggle-aux-pane',
          payload: request
        }, '*');
      },
      toggleConversation(convId) {
        window.parent.postMessage({
          type: 'toggle-conversation',
          payload: { conversationId: convId }
        }, '*');
      }
    }
  };
})();
