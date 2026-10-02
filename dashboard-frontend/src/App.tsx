/**
 * App shell (D9 §"Hash router + layout shell"): stats-strip slot (top), scrubber slot
 * (under it), view router with the four routes. Gated by auth — the login shell renders
 * when unauthenticated; a 401 anywhere routes through `teardownSession()` (cache cleared,
 * stores reset, WS closed) which flips auth off and returns to the login shell.
 */
import { useEffect } from 'react';
import { getConnection, teardownSession } from './api/connection';
import { useAuth } from './store/hooks';
import { LoginShell } from './components/LoginShell';
import { NavTabs } from './components/NavTabs';
import { StatsStrip } from './components/StatsStrip/StatsStrip';
import { Scrubber } from './components/Scrubber/Scrubber';
import { useHashRoute } from './router/useHashRoute';
import { VIEW_BY_ROUTE } from './views/registry';

export function App() {
  const authed = useAuth((s) => s.authenticated);
  const { client } = getConnection();

  if (!authed) {
    return <LoginShell client={client} />;
  }
  return <Dashboard />;
}

function Dashboard() {
  const { client, socket } = getConnection();
  const route = useHashRoute();
  const ActiveView = VIEW_BY_ROUTE[route];

  // Open the WS once on mount; close on unmount. StrictMode double-mounts in dev — the
  // socket.connect()/disconnect() pair is idempotent so no duplicate pipe leaks.
  useEffect(() => {
    socket.connect();
    return () => socket.disconnect();
  }, [socket]);

  async function onLogout() {
    try {
      await client.logout();
    } finally {
      // Centralized teardown: clears the REST cache + resets both stores + closes the WS,
      // so no session-scoped data survives logout (finding 1).
      teardownSession();
    }
  }

  return (
    <div className="flex h-full flex-col bg-bg text-text">
      <NavTabs active={route} onLogout={onLogout} />
      {/* stats-strip slot */}
      <StatsStrip />
      {/* Access is current authorization state, not a historical telemetry surface. */}
      {route !== 'access' && route !== 'chat' && <Scrubber socket={socket} />}
      {/* view router */}
      <main className="flex flex-1 overflow-hidden">
        <ActiveView />
      </main>
    </div>
  );
}
