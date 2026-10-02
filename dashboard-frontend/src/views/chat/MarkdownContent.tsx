import { Fragment, type ReactNode } from 'react';
import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import javascript from 'highlight.js/lib/languages/javascript';
import json from 'highlight.js/lib/languages/json';
import markdown from 'highlight.js/lib/languages/markdown';
import python from 'highlight.js/lib/languages/python';
import rust from 'highlight.js/lib/languages/rust';
import typescript from 'highlight.js/lib/languages/typescript';

const LANGUAGES = { bash, javascript, json, markdown, python, rust, typescript };
const LANGUAGE_ALIASES: Record<string, keyof typeof LANGUAGES> = {
  sh: 'bash', shell: 'bash', js: 'javascript', jsx: 'javascript', jsonc: 'json',
  md: 'markdown', py: 'python', rs: 'rust', ts: 'typescript', tsx: 'typescript',
};

let languagesRegistered = false;

function ensureLanguages(): void {
  if (languagesRegistered) return;
  for (const [name, language] of Object.entries(LANGUAGES)) hljs.registerLanguage(name, language);
  languagesRegistered = true;
}

export function MarkdownContent({ content, compact = false }: { content: string; compact?: boolean }) {
  const parts: ReactNode[] = [];
  const fence = /```([^\n`]*)\n?([\s\S]*?)```/g;
  let cursor = 0;
  let match: RegExpExecArray | null;

  while ((match = fence.exec(content)) !== null) {
    if (match.index > cursor) parts.push(<Fragment key={`prose-${cursor}`}>{renderProse(content.slice(cursor, match.index))}</Fragment>);
    const requested = (match[1] ?? '').trim().toLowerCase();
    const code = (match[2] ?? '').replace(/\n$/, '');
    parts.push(<CodeBlock key={`code-${match.index}`} code={code} language={requested} />);
    cursor = match.index + match[0].length;
  }
  if (cursor < content.length) parts.push(<Fragment key={`prose-${cursor}`}>{renderProse(content.slice(cursor))}</Fragment>);

  return <div className={`chat-markdown break-words text-sm leading-relaxed text-text ${compact ? 'chat-markdown-compact' : ''}`}>{parts}</div>;
}

function CodeBlock({ code, language }: { code: string; language: string }) {
  ensureLanguages();
  const resolved = (LANGUAGE_ALIASES[language] ?? language) as keyof typeof LANGUAGES;
  let highlighted = escapeHtml(code);
  if (resolved in LANGUAGES) {
    highlighted = hljs.highlight(code, { language: resolved, ignoreIllegals: true }).value;
  }
  return (
    <div className="my-3 overflow-hidden rounded border border-line bg-panel-raised" data-testid="chat-code-block">
      {language && <div className="border-b border-line px-3 py-1 font-mono text-[9px] uppercase tracking-wide text-text-muted">{language}</div>}
      <pre className="overflow-x-auto p-3 text-xs leading-relaxed"><code className="hljs" dangerouslySetInnerHTML={{ __html: highlighted }} /></pre>
    </div>
  );
}

function renderProse(source: string): ReactNode[] {
  const lines = source.split('\n');
  const nodes: ReactNode[] = [];
  let index = 0;
  while (index < lines.length) {
    const line = lines[index] ?? '';
    if (!line.trim()) { index += 1; continue; }

    const heading = /^(#{1,4})\s+(.+)$/.exec(line);
    if (heading) {
      const level = heading[1]?.length ?? 1;
      const className = level === 1 ? 'mt-4 mb-2 text-lg font-semibold' : 'mt-3 mb-1.5 font-semibold';
      nodes.push(<div key={index} role="heading" aria-level={level} className={className}>{renderInline(heading[2] ?? '')}</div>);
      index += 1;
      continue;
    }

    if (/^\s*[-*+]\s+/.test(line)) {
      const items: ReactNode[] = [];
      while (index < lines.length && /^\s*[-*+]\s+/.test(lines[index] ?? '')) {
        items.push(<li key={index}>{renderInline((lines[index] ?? '').replace(/^\s*[-*+]\s+/, ''))}</li>);
        index += 1;
      }
      nodes.push(<ul key={`ul-${index}`} className="my-2 list-disc space-y-1 pl-5">{items}</ul>);
      continue;
    }

    if (/^\s*\d+[.)]\s+/.test(line)) {
      const items: ReactNode[] = [];
      while (index < lines.length && /^\s*\d+[.)]\s+/.test(lines[index] ?? '')) {
        items.push(<li key={index}>{renderInline((lines[index] ?? '').replace(/^\s*\d+[.)]\s+/, ''))}</li>);
        index += 1;
      }
      nodes.push(<ol key={`ol-${index}`} className="my-2 list-decimal space-y-1 pl-5">{items}</ol>);
      continue;
    }

    if (/^>\s?/.test(line)) {
      const quote: string[] = [];
      while (index < lines.length && /^>\s?/.test(lines[index] ?? '')) {
        quote.push((lines[index] ?? '').replace(/^>\s?/, ''));
        index += 1;
      }
      nodes.push(<blockquote key={`quote-${index}`} className="my-2 border-l-2 border-accent/50 pl-3 text-text-muted">{quote.map((text, i) => <Fragment key={i}>{i > 0 && <br />}{renderInline(text)}</Fragment>)}</blockquote>);
      continue;
    }

    const paragraph = [line];
    index += 1;
    while (index < lines.length && (lines[index] ?? '').trim() && !/^(#{1,4})\s|^\s*[-*+]\s+|^\s*\d+[.)]\s+|^>\s?/.test(lines[index] ?? '')) {
      paragraph.push(lines[index] ?? '');
      index += 1;
    }
    nodes.push(<p key={`p-${index}`} className="my-2">{paragraph.map((text, i) => <Fragment key={i}>{i > 0 && <br />}{renderInline(text)}</Fragment>)}</p>);
  }
  return nodes;
}

function renderInline(text: string): ReactNode[] {
  const pattern = /(`[^`]+`|\*\*[^*]+\*\*|__[^_]+__|\*[^*]+\*|_([^_]+)_|\[[^\]]+\]\([^)]+\))/g;
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(text)) !== null) {
    if (match.index > cursor) nodes.push(text.slice(cursor, match.index));
    const token = match[0];
    if (token.startsWith('`')) nodes.push(<code key={match.index} className="rounded bg-panel-raised px-1 py-0.5 font-mono text-[0.9em] text-meta">{token.slice(1, -1)}</code>);
    else if (token.startsWith('**') || token.startsWith('__')) nodes.push(<strong key={match.index} className="font-semibold text-text">{token.slice(2, -2)}</strong>);
    else if (token.startsWith('*') || token.startsWith('_')) nodes.push(<em key={match.index}>{token.slice(1, -1)}</em>);
    else {
      const link = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(token);
      const href = safeHref(link?.[2] ?? '');
      nodes.push(href ? <a key={match.index} href={href} target="_blank" rel="noreferrer" className="text-accent underline decoration-accent/40 underline-offset-2">{link?.[1]}</a> : token);
    }
    cursor = match.index + token.length;
  }
  if (cursor < text.length) nodes.push(text.slice(cursor));
  return nodes;
}

function safeHref(value: string): string | null {
  return /^(https?:|mailto:)/i.test(value.trim()) ? value.trim() : null;
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' })[character] ?? character);
}
