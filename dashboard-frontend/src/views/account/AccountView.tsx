/**
 * AccountView — who you are, your API keys (create / revoke; the secret is shown once), and,
 * for administrators, user management (create, reset password, role, delete) plus everyone's
 * keys. Mutations carry the double-submit CSRF token through the client.
 */
import { useState, type FormEvent } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import { useAuth } from '../../store/hooks';
import type { ApiKeyRecord, UserRecord } from '../../api/types';
import { Panel } from '../../components/ui/Panel';
import { Button } from '../../components/ui/Button';
import { cn } from '../../lib/cn';

const DASH = '—';

function fmtWhen(ms: number): string {
  return new Date(ms).toISOString().slice(0, 16).replace('T', ' ');
}

export function AccountView() {
  const { client } = getConnection();
  const queryClient = useQueryClient();
  const user = useAuth((s) => s.user);
  const authMode = useAuth((s) => s.authMode);
  const isAdmin = user?.is_admin ?? authMode !== 'users';
  const me = useQuery({ queryKey: queryKeys.me, queryFn: () => client.me(), retry: false });
  const accountsEnabled = me.data?.accounts_enabled ?? true;
  const keys = useQuery({ queryKey: queryKeys.keys(isAdmin ? 'all' : 'mine'), queryFn: () => client.listKeys(isAdmin ? 'all' : user?.id), retry: false, enabled: accountsEnabled });
  const users = useQuery({ queryKey: queryKeys.users, queryFn: () => client.listUsers(), enabled: isAdmin && accountsEnabled, retry: false });
  const [secret, setSecret] = useState<{ label: string | null; secret: string } | null>(null);
  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['keys'] });
    void queryClient.invalidateQueries({ queryKey: queryKeys.users });
  };
  const createKey = useMutation({
    mutationFn: (body: { label?: string; allowed_models?: string[]; user_id?: string }) => client.createKey(body),
    onSuccess: (created) => {
      setSecret({ label: created.key.label, secret: created.secret });
      invalidate();
    },
  });
  const revokeKey = useMutation({ mutationFn: (id: string) => client.deleteKey(id), onSuccess: invalidate });
  const createUser = useMutation({ mutationFn: (body: { username: string; password: string; is_admin: boolean }) => client.createUser(body), onSuccess: invalidate });
  const updateUser = useMutation({ mutationFn: (args: { id: string; password?: string; is_admin?: boolean }) => client.updateUser(args.id, { password: args.password, is_admin: args.is_admin }), onSuccess: invalidate });
  const deleteUser = useMutation({ mutationFn: (id: string) => client.deleteUser(id), onSuccess: invalidate });
  const userName = (id: string | null) => (id == null ? DASH : users.data?.users.find((u) => u.id === id)?.username ?? (user && user.id === id ? user.username : id.slice(0, 8)));

  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-auto p-4" data-testid="account-view">
      <div className="mb-3 flex items-baseline gap-2">
        <h1 className="text-sm font-semibold uppercase tracking-[0.18em] text-text">account</h1>
        <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">you · keys · users</span>
      </div>

      <Panel raised className="mb-3 flex flex-wrap items-center gap-4 px-3 py-2" data-testid="account-me">
        <span className="font-mono text-sm text-text" data-testid="account-username">{user?.username ?? (authMode === 'open' ? 'dev-open session' : 'token session')}</span>
        <span className={cn('rounded-sm px-1 text-[9px] uppercase tracking-wide', isAdmin ? 'bg-accent/15 text-accent' : 'bg-line/40 text-text-muted')} data-testid="account-role">
          {isAdmin ? 'admin' : 'user'}
        </span>
        <span className="text-xs text-text-muted" data-testid="account-auth-mode">auth: {me.data?.auth_mode ?? authMode}</span>
        {!accountsEnabled && (
          <span className="text-xs text-status-cooling" data-testid="account-disabled">
            user and key management need a SQL store (control_plane.storage sqlite or postgres)
          </span>
        )}
      </Panel>

      {secret && (
        <Panel className="mb-3 border-status-healthy/40 px-3 py-2" data-testid="account-new-secret">
          <div className="text-[10px] uppercase tracking-[0.14em] text-status-healthy">new key{secret.label ? ` · ${secret.label}` : ''} — copy it now, it is shown once</div>
          <code className="mt-1 block select-all break-all font-mono text-sm text-text" data-testid="account-secret">{secret.secret}</code>
          <button type="button" className="mt-1 text-[10px] text-text-muted hover:text-text" onClick={() => setSecret(null)}>dismiss</button>
        </Panel>
      )}

      <section className="mb-4" data-testid="account-keys">
        <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">{isAdmin ? 'api keys · all users' : 'my api keys'}</h2>
        <KeyForm
          busy={createKey.isPending}
          error={createKey.error ? String(createKey.error) : null}
          admin={isAdmin}
          users={users.data?.users ?? []}
          onCreate={(body) => createKey.mutate(body)}
        />
        <KeyTable keys={keys.data?.keys ?? []} showOwner={isAdmin} ownerName={userName} onRevoke={(id) => revokeKey.mutate(id)} busy={revokeKey.isPending} />
      </section>

      {isAdmin && (
        <section data-testid="account-users">
          <h2 className="mb-1 text-[10px] uppercase tracking-[0.14em] text-text-muted">users</h2>
          <UserForm busy={createUser.isPending} error={createUser.error ? String(createUser.error) : null} onCreate={(body) => createUser.mutate(body)} />
          <UserTable
            users={users.data?.users ?? []}
            selfId={user?.id ?? null}
            keys={keys.data?.keys ?? []}
            onResetPassword={(id, password) => updateUser.mutate({ id, password })}
            onToggleAdmin={(id, is_admin) => updateUser.mutate({ id, is_admin })}
            onDelete={(id) => deleteUser.mutate(id)}
          />
          {(updateUser.error || deleteUser.error) && <p className="mt-1 text-xs text-status-down" data-testid="account-users-error">{String(updateUser.error ?? deleteUser.error)}</p>}
        </section>
      )}
    </div>
  );
}

const INPUT = 'rounded-md border border-line bg-panel-raised px-2 py-1 font-mono text-xs text-text outline-none focus:border-accent';

function KeyForm({ busy, error, admin, users, onCreate }: { busy: boolean; error: string | null; admin: boolean; users: UserRecord[]; onCreate: (body: { label?: string; allowed_models?: string[]; user_id?: string }) => void }) {
  const [label, setLabel] = useState('');
  const [models, setModels] = useState('');
  const [owner, setOwner] = useState('');
  const submit = (e: FormEvent) => {
    e.preventDefault();
    onCreate({
      label: label.trim() || undefined,
      allowed_models: models.split(',').map((m) => m.trim()).filter(Boolean),
      user_id: owner || undefined,
    });
    setLabel('');
    setModels('');
  };
  return (
    <form onSubmit={submit} className="mb-2 flex flex-wrap items-center gap-2" data-testid="key-form">
      <input className={INPUT} placeholder="label (e.g. laptop)" aria-label="key label" value={label} onChange={(e) => setLabel(e.target.value)} />
      <input className={cn(INPUT, 'w-56')} placeholder="allowed models, comma-separated (empty = any)" aria-label="allowed models" value={models} onChange={(e) => setModels(e.target.value)} />
      {admin && users.length > 0 && (
        <select className={INPUT} aria-label="key owner" value={owner} onChange={(e) => setOwner(e.target.value)}>
          <option value="">owner: me</option>
          {users.map((u) => (
            <option key={u.id} value={u.id}>owner: {u.username}</option>
          ))}
        </select>
      )}
      <Button type="submit" disabled={busy}>{busy ? 'creating…' : 'create key'}</Button>
      {error && <span className="text-xs text-status-down" data-testid="key-form-error">{error}</span>}
    </form>
  );
}

function KeyTable({ keys, showOwner, ownerName, onRevoke, busy }: { keys: ApiKeyRecord[]; showOwner: boolean; ownerName: (id: string | null) => string; onRevoke: (id: string) => void; busy: boolean }) {
  if (keys.length === 0) return <div className="rounded-md border border-line px-3 py-3 text-xs italic text-text-muted" data-testid="keys-empty">No keys yet.</div>;
  return (
    <div className="rounded-md border border-line">
      <div className="grid grid-cols-[minmax(120px,1fr)_minmax(100px,1fr)_minmax(120px,1fr)_130px_80px] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
        <span>label</span><span>owner</span><span>allowed models</span><span>created</span><span />
      </div>
      {keys.map((k) => (
        <div key={k.id} className="grid grid-cols-[minmax(120px,1fr)_minmax(100px,1fr)_minmax(120px,1fr)_130px_80px] items-center gap-2 border-b border-line/50 px-3 py-1 text-xs" data-testid="key-row" title={k.id}>
          <span className="truncate">{k.label ?? <span className="italic text-text-muted">unlabelled</span>}</span>
          <span className="truncate text-text-muted">{showOwner ? ownerName(k.user_id) : ownerName(k.user_id)}</span>
          <span className="truncate font-mono text-text-muted">{k.allowed_models.length ? k.allowed_models.join(', ') : 'any'}</span>
          <span className="tabular-nums text-text-muted">{fmtWhen(k.created_at_ms)}</span>
          <Button variant="danger" className="px-2 py-0.5 text-[11px]" disabled={busy} onClick={() => onRevoke(k.id)} data-testid="key-revoke">revoke</Button>
        </div>
      ))}
    </div>
  );
}

function UserForm({ busy, error, onCreate }: { busy: boolean; error: string | null; onCreate: (body: { username: string; password: string; is_admin: boolean }) => void }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [admin, setAdmin] = useState(false);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    onCreate({ username: username.trim(), password, is_admin: admin });
    setUsername('');
    setPassword('');
    setAdmin(false);
  };
  return (
    <form onSubmit={submit} className="mb-2 flex flex-wrap items-center gap-2" data-testid="user-form">
      <input className={INPUT} placeholder="username" aria-label="new username" autoComplete="off" value={username} onChange={(e) => setUsername(e.target.value)} />
      <input className={INPUT} type="password" placeholder="password (8+ chars)" aria-label="new password" autoComplete="new-password" value={password} onChange={(e) => setPassword(e.target.value)} />
      <label className="flex items-center gap-1 text-xs text-text-muted">
        <input type="checkbox" checked={admin} onChange={(e) => setAdmin(e.target.checked)} aria-label="administrator" /> admin
      </label>
      <Button type="submit" disabled={busy || username.length === 0 || password.length < 8}>{busy ? 'creating…' : 'create user'}</Button>
      {error && <span className="text-xs text-status-down" data-testid="user-form-error">{error}</span>}
    </form>
  );
}

function UserTable({ users, selfId, keys, onResetPassword, onToggleAdmin, onDelete }: { users: UserRecord[]; selfId: string | null; keys: ApiKeyRecord[]; onResetPassword: (id: string, password: string) => void; onToggleAdmin: (id: string, is_admin: boolean) => void; onDelete: (id: string) => void }) {
  const [resetFor, setResetFor] = useState<string | null>(null);
  const [newPassword, setNewPassword] = useState('');
  if (users.length === 0) return <div className="rounded-md border border-line px-3 py-3 text-xs italic text-text-muted" data-testid="users-empty">No users yet: create the first administrator above (or via the CLI).</div>;
  return (
    <div className="rounded-md border border-line">
      <div className="grid grid-cols-[minmax(120px,1fr)_70px_60px_130px_minmax(200px,1fr)] gap-2 border-b border-line bg-panel-raised px-3 py-1.5 text-[10px] uppercase tracking-[0.14em] text-text-muted">
        <span>username</span><span>role</span><span className="text-right">keys</span><span>created</span><span />
      </div>
      {users.map((u) => {
        const self = u.id === selfId;
        return (
          <div key={u.id} className="grid grid-cols-[minmax(120px,1fr)_70px_60px_130px_minmax(200px,1fr)] items-center gap-2 border-b border-line/50 px-3 py-1 text-xs" data-testid="user-row" data-admin={u.is_admin ? 'true' : 'false'}>
            <span className="truncate font-mono">{u.username}{self && <span className="ml-1 text-[9px] text-text-muted">(you)</span>}</span>
            <span className={cn('w-fit rounded-sm px-1 text-[9px] uppercase tracking-wide', u.is_admin ? 'bg-accent/15 text-accent' : 'bg-line/40 text-text-muted')}>{u.is_admin ? 'admin' : 'user'}</span>
            <span className="text-right tabular-nums text-text-muted">{keys.filter((k) => k.user_id === u.id).length}</span>
            <span className="tabular-nums text-text-muted">{fmtWhen(u.created_at_ms)}</span>
            <span className="flex flex-wrap items-center gap-1">
              {resetFor === u.id ? (
                <>
                  <input className={INPUT} type="password" placeholder="new password" aria-label={`new password for ${u.username}`} value={newPassword} onChange={(e) => setNewPassword(e.target.value)} />
                  <Button className="px-2 py-0.5 text-[11px]" disabled={newPassword.length < 8} onClick={() => { onResetPassword(u.id, newPassword); setResetFor(null); setNewPassword(''); }}>save</Button>
                  <Button variant="ghost" className="px-2 py-0.5 text-[11px]" onClick={() => setResetFor(null)}>cancel</Button>
                </>
              ) : (
                <>
                  <Button variant="ghost" className="px-2 py-0.5 text-[11px]" onClick={() => setResetFor(u.id)} data-testid="user-reset">reset password</Button>
                  <Button variant="ghost" className="px-2 py-0.5 text-[11px]" disabled={self} onClick={() => onToggleAdmin(u.id, !u.is_admin)} data-testid="user-toggle-admin">{u.is_admin ? 'make user' : 'make admin'}</Button>
                  <Button variant="danger" className="px-2 py-0.5 text-[11px]" disabled={self} onClick={() => onDelete(u.id)} data-testid="user-delete">delete</Button>
                </>
              )}
            </span>
          </div>
        );
      })}
    </div>
  );
}
