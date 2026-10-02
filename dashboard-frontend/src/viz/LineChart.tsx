/**
 * LineChart — a small, dependency-free SVG multi-series line chart for the history views
 * (throughput, activity). Pure render: no canvas, no observers, so it is trivially testable
 * in jsdom and cheap to mount many times. Series share one x axis (epoch ms) and one y axis
 * (auto-scaled to the visible max; never lies with a fabricated baseline: an empty series
 * renders the explicit `—` placeholder rather than a flat line at 0).
 */
import { colors } from '../design/tokens';
import { seriesColor } from './seriesColor';

export interface ChartSeries {
  key: string;
  label: string;
  /** `[x epoch ms, y]` points, x ascending. `null` y = gap. */
  points: Array<[number, number | null]>;
  stroke?: string;
}


export function LineChart({
  series,
  width = 640,
  height = 160,
  unit = '',
  testId,
  formatY = (v) => (Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1)}k` : v.toFixed(v < 10 ? 1 : 0)),
}: {
  series: ChartSeries[];
  width?: number;
  height?: number;
  unit?: string;
  testId?: string;
  formatY?: (v: number) => string;
}) {
  const pad = { l: 44, r: 8, t: 8, b: 20 };
  const xs = series.flatMap((s) => s.points.map((p) => p[0]));
  const ys = series.flatMap((s) => s.points.map((p) => p[1]).filter((y): y is number => y != null));
  const hasData = xs.length > 0 && ys.length > 0;
  const xMin = hasData ? Math.min(...xs) : 0;
  const xMax = hasData ? Math.max(...xs) : 1;
  const yMax = hasData ? Math.max(...ys, 0) : 1;
  const innerW = width - pad.l - pad.r;
  const innerH = height - pad.t - pad.b;
  const sx = (x: number) => pad.l + (xMax === xMin ? innerW / 2 : ((x - xMin) / (xMax - xMin)) * innerW);
  const sy = (y: number) => pad.t + innerH - (yMax === 0 ? 0 : (y / yMax) * innerH);
  const ticks = [0, 0.5, 1].map((f) => yMax * f);
  const fmtX = (x: number) => {
    const d = new Date(x);
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  };
  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="h-auto w-full"
      role="img"
      aria-label={series.map((s) => s.label).join(', ') || 'empty chart'}
      data-testid={testId}
      data-available={hasData ? 'true' : 'false'}
    >
      {ticks.map((t, i) => (
        <g key={i}>
          <line x1={pad.l} x2={width - pad.r} y1={sy(t)} y2={sy(t)} stroke={colors.line} strokeWidth={1} />
          <text x={pad.l - 6} y={sy(t) + 3} fontSize={9} textAnchor="end" fill={colors.textMuted} fontFamily="monospace">
            {formatY(t)}{unit}
          </text>
        </g>
      ))}
      {hasData && (
        <>
          <text x={pad.l} y={height - 6} fontSize={9} fill={colors.textMuted} fontFamily="monospace">{fmtX(xMin)}</text>
          <text x={width - pad.r} y={height - 6} fontSize={9} textAnchor="end" fill={colors.textMuted} fontFamily="monospace">{fmtX(xMax)}</text>
        </>
      )}
      {!hasData && (
        <text x={width / 2} y={height / 2} fontSize={11} textAnchor="middle" fill={colors.textMuted}>—</text>
      )}
      {series.map((s, i) => {
        const stroke = s.stroke ?? seriesColor(i);
        let d = '';
        let pen = false;
        for (const [x, y] of s.points) {
          if (y == null) {
            pen = false;
            continue;
          }
          d += `${pen ? 'L' : 'M'}${sx(x).toFixed(1)},${sy(y).toFixed(1)} `;
          pen = true;
        }
        return <path key={s.key} d={d} fill="none" stroke={stroke} strokeWidth={1.5} data-series={s.key} />;
      })}
    </svg>
  );
}

/** Legend row for a chart's series (same color order as the chart). */
export function ChartLegend({ series }: { series: ChartSeries[] }) {
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-text-muted">
      {series.map((s, i) => (
        <span key={s.key} className="flex items-center gap-1">
          <span className="inline-block h-1.5 w-3 rounded-sm" style={{ background: s.stroke ?? seriesColor(i) }} />
          {s.label}
        </span>
      ))}
    </div>
  );
}
