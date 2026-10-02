import { describe, expect, it } from 'vitest';
import { AccessView } from './access/AccessView';
import { ProvidersView } from './providers/ProvidersView';
import { ChatView } from './chat/ChatView';
import { VIEW_BY_ROUTE } from './registry';

describe('view registry', () => {
  it('routes the access tab to the complete management console', () => {
    expect(VIEW_BY_ROUTE.access).toBe(AccessView);
  });

  it('routes the providers tab to the provider inventory', () => {
    expect(VIEW_BY_ROUTE.providers).toBe(ProvidersView);
  });

  it('routes the chat tab to the chat view', () => {
    expect(VIEW_BY_ROUTE.chat).toBe(ChatView);
  });
});
