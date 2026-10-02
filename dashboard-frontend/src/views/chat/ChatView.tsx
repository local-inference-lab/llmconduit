import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import type { DashboardChatMessage, DashboardChatResult } from '../../api/client';
import { Button } from '../../components/ui/Button';
import { Panel } from '../../components/ui/Panel';
import { cn } from '../../lib/cn';
import { MarkdownContent } from './MarkdownContent';

type ThinkingLevel = 'low' | 'medium' | 'high' | 'xhigh' | 'max';
type ChatEntry = DashboardChatMessage & { id: number; reasoning?: string };
type RunState = 'idle' | 'streaming' | 'complete' | 'stopped' | 'error';
type RunRates = { tgPerSecond: number | null; ppPerSecond: number | null };

const DEFAULT_MAX_CONTEXT_LENGTH = 256 * 1024;

export function ChatView() {
  const { client } = getConnection();
  const catalog = useQuery({ queryKey: queryKeys.catalog, queryFn: () => client.catalog() });
  const [model, setModel] = useState('');
  const [systemPrompt, setSystemPrompt] = useState('You are a concise, helpful assistant.');
  const [thinkingLevel, setThinkingLevel] = useState<ThinkingLevel>('medium');
  const [temperature, setTemperature] = useState('1');
  const [topP, setTopP] = useState('0.95');
  const [maxContextOverride, setMaxContextOverride] = useState<string | null>(null);
  const [prompt, setPrompt] = useState('');
  const [messages, setMessages] = useState<ChatEntry[]>([]);
  const [runState, setRunState] = useState<RunState>('idle');
  const [result, setResult] = useState<DashboardChatResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elapsedMs, setElapsedMs] = useState<number | null>(null);
  const [rates, setRates] = useState<RunRates>({ tgPerSecond: null, ppPerSecond: null });
  const abortRef = useRef<AbortController | null>(null);
  const transcriptRef = useRef<HTMLDivElement | null>(null);
  const nextId = useRef(1);
  const promptCacheKeyRef = useRef(newPromptCacheKey());

  const models = useMemo(() => catalog.data?.map((entry) => entry.id) ?? [], [catalog.data]);
  const advertisedContextLimit = catalog.data?.find((entry) => entry.id === model)?.context_limit ?? null;
  const defaultContextLength = advertisedContextLimit !== null && Number.isSafeInteger(advertisedContextLimit) && advertisedContextLimit > 0
    ? Math.min(DEFAULT_MAX_CONTEXT_LENGTH, advertisedContextLimit)
    : DEFAULT_MAX_CONTEXT_LENGTH;
  const maxContextLength = maxContextOverride ?? String(defaultContextLength);
  useEffect(() => {
    if (!model && models[0]) setModel(models[0]);
  }, [model, models]);
  useEffect(() => {
    const transcript = transcriptRef.current;
    if (transcript) transcript.scrollTop = transcript.scrollHeight;
  }, [messages]);
  useEffect(() => () => abortRef.current?.abort(), []);

  const busy = runState === 'streaming';

  async function send() {
    const text = prompt.trim();
    const selectedModel = model.trim();
    if (!text || !selectedModel || busy) return;

    const user: ChatEntry = { id: nextId.current++, role: 'user', content: text };
    const assistant: ChatEntry = { id: nextId.current++, role: 'assistant', content: '' };
    const history: DashboardChatMessage[] = [
      ...(systemPrompt.trim() ? [{ role: 'system' as const, content: systemPrompt.trim() }] : []),
      ...messages.map(({ role, content }) => ({ role, content })),
      { role: 'user', content: text },
    ];
    const controller = new AbortController();
    abortRef.current = controller;
    setMessages((current) => [...current, user, assistant]);
    setPrompt('');
    setResult(null);
    setError(null);
    setElapsedMs(null);
    setRates({ tgPerSecond: null, ppPerSecond: null });
    setRunState('streaming');
    const started = performance.now();
    let firstTokenAt: number | null = null;

    try {
      const completed = await client.streamChat(
        {
          model: selectedModel,
          messages: history,
          temperature: parseOptionalNumber(temperature),
          top_p: parseOptionalNumber(topP),
          max_tokens: parseOptionalInteger(maxContextLength),
          reasoning_effort: thinkingLevel,
          prompt_cache_key: promptCacheKeyRef.current,
        },
        (delta) => {
          if (delta.text && firstTokenAt === null) firstTokenAt = performance.now();
          setMessages((current) => current.map((entry) => (
            entry.id === assistant.id
              ? delta.kind === 'reasoning'
                ? { ...entry, reasoning: (entry.reasoning ?? '') + delta.text }
                : { ...entry, content: entry.content + delta.text }
              : entry
          )));
        },
        controller.signal,
      );
      const finished = performance.now();
      setResult(completed);
      setRates(calculateRunRates(completed, started, firstTokenAt, finished));
      setRunState('complete');
    } catch (caught) {
      if (controller.signal.aborted) {
        setRunState('stopped');
      } else {
        setError(caught instanceof Error ? caught.message : String(caught));
        setRunState('error');
      }
    } finally {
      setElapsedMs(Math.round(performance.now() - started));
      if (abortRef.current === controller) abortRef.current = null;
    }
  }

  function stop() {
    abortRef.current?.abort();
  }

  function clear() {
    abortRef.current?.abort();
    setMessages([]);
    setResult(null);
    setError(null);
    setElapsedMs(null);
    setRates({ tgPerSecond: null, ppPerSecond: null });
    setRunState('idle');
    promptCacheKeyRef.current = newPromptCacheKey();
  }

  function onComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      void send();
    }
  }

  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 overflow-hidden p-4" data-testid="chat-view">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="font-ui text-sm font-semibold uppercase tracking-[0.16em] text-text">Chat</h1>
          <p className="mt-1 text-[11px] text-text-muted">Chat with any model currently advertised by the gateway.</p>
        </div>
        <RunStatus state={runState} result={result} elapsedMs={elapsedMs} rates={rates} />
      </header>

      <div className="grid min-h-0 flex-1 gap-3 lg:grid-cols-[280px_minmax(0,1fr)]">
        <Panel className="min-h-0 overflow-auto p-3" data-testid="chat-settings">
          <SectionLabel>Target</SectionLabel>
          <label className="mt-2 block text-[10px] uppercase tracking-wide text-text-muted" htmlFor="chat-model">Model</label>
          <select
            id="chat-model"
            value={model}
            onChange={(event) => { setModel(event.target.value); setMaxContextOverride(null); }}
            disabled={busy || models.length === 0}
            className="mt-1 w-full rounded border border-line bg-bg px-2.5 py-2 font-mono text-xs text-text outline-none focus:border-accent"
          >
            {models.length === 0 && <option value="">{catalog.isLoading ? 'Loading models…' : 'No models available'}</option>}
            {models.map((id) => <option key={id} value={id}>{id}</option>)}
          </select>
          {catalog.isError && <p className="mt-1 text-[10px] text-status-down">Model catalog unavailable.</p>}

          <label className="mt-3 block text-[10px] uppercase tracking-wide text-text-muted" htmlFor="chat-thinking-level">Thinking level</label>
          <select
            id="chat-thinking-level"
            value={thinkingLevel}
            onChange={(event) => setThinkingLevel(event.target.value as ThinkingLevel)}
            disabled={busy}
            className="mt-1 w-full rounded border border-line bg-bg px-2.5 py-2 font-mono text-xs text-text outline-none focus:border-accent"
          >
            {(['low', 'medium', 'high', 'xhigh', 'max'] as const).map((level) => <option key={level} value={level}>{level}</option>)}
          </select>

          <label className="mt-4 block text-[10px] uppercase tracking-wide text-text-muted" htmlFor="chat-system">System prompt</label>
          <textarea
            id="chat-system"
            value={systemPrompt}
            onChange={(event) => setSystemPrompt(event.target.value)}
            disabled={busy}
            rows={4}
            className="mt-1 w-full resize-y rounded border border-line bg-bg px-2.5 py-2 text-xs leading-relaxed text-text outline-none focus:border-accent"
          />

          <div className="mt-4 grid grid-cols-2 gap-2">
            <NumberField label="Temperature" value={temperature} onChange={setTemperature} disabled={busy} />
            <NumberField label="Top P" value={topP} onChange={setTopP} disabled={busy} />
          </div>
          <div className="mt-3">
            <NumberField label="Max context length" value={maxContextLength} onChange={setMaxContextOverride} disabled={busy} />
            <p className="mt-1 text-[9px] leading-relaxed text-text-muted">
              Maximum generated tokens for this turn{advertisedContextLimit ? ` · model window ${advertisedContextLimit.toLocaleString()}` : ''}.
            </p>
          </div>
        </Panel>

        <Panel className="flex min-h-0 min-w-0 flex-col overflow-hidden" raised data-testid="chat-console">
          <div className="flex items-center justify-between border-b border-line px-3 py-2">
            <div className="flex items-center gap-2">
              <span className={cn('h-2 w-2 rounded-full', statusColor(runState))} aria-hidden />
              <span className="text-[10px] uppercase tracking-[0.14em] text-text-muted">conversation</span>
            </div>
            <Button type="button" variant="ghost" className="px-2 py-1 text-[10px]" onClick={clear} disabled={messages.length === 0 && !busy}>Clear</Button>
          </div>

          <div ref={transcriptRef} className="min-h-0 flex-1 space-y-3 overflow-auto p-4" aria-live="polite">
            {messages.length === 0 ? <EmptyChat /> : messages.map((entry) => (
              <article key={entry.id} className={cn('flex', entry.role === 'user' ? 'justify-end' : 'justify-start')} data-testid={`chat-message-${entry.role}`}>
                <div className="max-w-[86%] space-y-2">
                  {entry.role === 'assistant' && entry.reasoning && (
                    <details className="group rounded-md border border-meta/35 bg-meta/5" open data-testid="chat-thinking">
                      <summary className="cursor-pointer select-none px-3 py-2 text-[10px] font-medium uppercase tracking-[0.14em] text-meta marker:text-meta">
                        Thinking
                      </summary>
                      <div className="border-t border-meta/20 px-3 py-2 text-text-muted">
                        <MarkdownContent content={entry.reasoning} compact />
                      </div>
                    </details>
                  )}
                  <div className={cn(
                    'rounded-md border px-3 py-2',
                    entry.role === 'user' ? 'border-accent/40 bg-accent/10' : 'border-line bg-bg',
                  )}>
                    <div className="mb-1 text-[9px] uppercase tracking-[0.14em] text-text-muted">{entry.role}</div>
                    {entry.role === 'assistant' ? (
                      entry.content
                        ? <MarkdownContent content={entry.content} />
                        : busy && !entry.reasoning
                          ? <span className="animate-pulse text-sm text-text-muted">Waiting for first token…</span>
                          : !busy && <span className="text-sm text-text-muted">No output received.</span>
                    ) : (
                      <div className="whitespace-pre-wrap break-words text-sm leading-relaxed text-text">{entry.content}</div>
                    )}
                  </div>
                </div>
              </article>
            ))}
            {error && (
              <div className="rounded border border-status-down/40 bg-status-down/10 px-3 py-2 text-xs text-status-down" role="alert" data-testid="chat-error">
                {error}
              </div>
            )}
          </div>

          <div className="border-t border-line bg-panel p-3">
            <textarea
              aria-label="Message"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              onKeyDown={onComposerKeyDown}
              disabled={busy}
              rows={3}
              placeholder="Ask the model something…  Enter to send, Shift+Enter for a new line"
              className="w-full resize-none rounded border border-line bg-bg px-3 py-2 text-sm leading-relaxed text-text outline-none placeholder:text-text-muted/60 focus:border-accent"
            />
            <div className="mt-2 flex items-center justify-between gap-3">
              <span className="truncate font-mono text-[10px] text-text-muted">{model || 'select a model'}</span>
              {busy ? (
                <Button type="button" variant="danger" onClick={stop} className="min-w-20">Stop</Button>
              ) : (
                <Button type="button" onClick={() => void send()} disabled={!prompt.trim() || !model.trim()} className="min-w-20">Send</Button>
              )}
            </div>
          </div>
        </Panel>
      </div>
    </div>
  );
}

function RunStatus({ state, result, elapsedMs, rates }: { state: RunState; result: DashboardChatResult | null; elapsedMs: number | null; rates: RunRates }) {
  const label = state === 'complete' ? `finish: ${result?.finishReason ?? 'unknown'}` : state;
  return (
    <div className="flex flex-wrap items-center justify-end gap-2 font-mono text-[10px]" data-testid="chat-run-status">
      {result?.usage && <span className="rounded border border-line px-2 py-1 text-text-muted">{result.usage.total_tokens} tokens</span>}
      {result?.usage && <span className="rounded border border-line px-2 py-1 text-text-muted">{formatRate(rates.tgPerSecond)} tg/s</span>}
      {result?.usage && <span className="rounded border border-line px-2 py-1 text-text-muted">{formatRate(rates.ppPerSecond)} pp/s</span>}
      {elapsedMs !== null && <span className="rounded border border-line px-2 py-1 text-text-muted">{(elapsedMs / 1000).toFixed(1)}s</span>}
      <span className={cn('rounded border px-2 py-1 uppercase tracking-wide', statusText(state))}>{label}</span>
    </div>
  );
}

function calculateRunRates(result: DashboardChatResult, started: number, firstTokenAt: number | null, finished: number): RunRates {
  if (!result.usage || firstTokenAt === null) return { tgPerSecond: null, ppPerSecond: null };
  const prefillSeconds = (firstTokenAt - started) / 1000;
  const generationSeconds = (finished - firstTokenAt) / 1000;
  return {
    tgPerSecond: generationSeconds > 0 ? result.usage.completion_tokens / generationSeconds : null,
    ppPerSecond: prefillSeconds > 0 ? result.usage.prompt_tokens / prefillSeconds : null,
  };
}

function formatRate(rate: number | null): string {
  if (rate === null || !Number.isFinite(rate)) return '—';
  return rate >= 100 ? rate.toFixed(0) : rate.toFixed(1);
}

function EmptyChat() {
  return (
    <div className="grid h-full min-h-48 place-items-center text-center" data-testid="chat-empty">
      <div>
        <div className="mx-auto mb-3 grid h-10 w-10 place-items-center rounded-full border border-accent/30 bg-accent/10 font-mono text-accent">›_</div>
        <p className="text-sm text-text">Start a conversation</p>
        <p className="mt-1 max-w-sm text-[11px] leading-relaxed text-text-muted">Choose a model and send a message.</p>
      </div>
    </div>
  );
}

function SectionLabel({ children }: { children: string }) {
  return <h2 className="text-[10px] uppercase tracking-[0.14em] text-text-muted">{children}</h2>;
}

function NumberField({ label, value, onChange, disabled }: { label: string; value: string; onChange: (value: string) => void; disabled: boolean }) {
  return (
    <label className="text-[10px] uppercase tracking-wide text-text-muted">
      {label}
      <input value={value} onChange={(event) => onChange(event.target.value)} disabled={disabled} inputMode="decimal" className="mt-1 w-full rounded border border-line bg-bg px-2 py-1.5 font-mono text-xs text-text outline-none focus:border-accent" />
    </label>
  );
}

function parseOptionalNumber(value: string): number | undefined {
  const parsed = Number(value);
  return value.trim() && Number.isFinite(parsed) ? parsed : undefined;
}

function parseOptionalInteger(value: string): number | undefined {
  const parsed = Number.parseInt(value, 10);
  return value.trim() && Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function newPromptCacheKey(): string {
  return globalThis.crypto?.randomUUID?.() ?? `dashboard-chat-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function statusColor(state: RunState): string {
  if (state === 'streaming') return 'animate-pulse bg-accent';
  if (state === 'complete') return 'bg-status-healthy';
  if (state === 'error') return 'bg-status-down';
  if (state === 'stopped') return 'bg-status-cooling';
  return 'bg-text-muted/50';
}

function statusText(state: RunState): string {
  if (state === 'complete') return 'border-status-healthy/40 bg-status-healthy/10 text-status-healthy';
  if (state === 'error') return 'border-status-down/40 bg-status-down/10 text-status-down';
  if (state === 'streaming') return 'border-accent/40 bg-accent/10 text-accent';
  if (state === 'stopped') return 'border-status-cooling/40 bg-status-cooling/10 text-status-cooling';
  return 'border-line text-text-muted';
}
