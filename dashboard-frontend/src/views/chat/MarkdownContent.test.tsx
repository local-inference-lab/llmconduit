import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { MarkdownContent } from './MarkdownContent';

afterEach(cleanup);

describe('MarkdownContent', () => {
  it('renders markdown structure and safe external links', () => {
    render(<MarkdownContent content={'## Result\n\n**Ready** with `code` and [docs](https://example.com).'} />);

    expect(screen.getByRole('heading', { name: 'Result' })).toHaveAttribute('aria-level', '2');
    expect(screen.getByText('Ready').tagName).toBe('STRONG');
    expect(screen.getByText('code').tagName).toBe('CODE');
    expect(screen.getByRole('link', { name: 'docs' })).toHaveAttribute('href', 'https://example.com');
  });

  it('syntax-highlights fenced code without rendering raw HTML', () => {
    const { container } = render(<MarkdownContent content={'```json\n{"safe":"<script>"}\n```'} />);

    expect(screen.getByTestId('chat-code-block')).toHaveTextContent('{"safe":"<script>"}');
    expect(container.querySelectorAll('.hljs-string').length).toBeGreaterThan(0);
    expect(container.querySelector('script')).toBeNull();
  });
});
