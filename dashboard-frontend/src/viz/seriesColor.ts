/** Series color order shared by the SVG charts and their legends (kept out of the .tsx for react-refresh). */
import { colors } from '../design/tokens';

const SERIES_COLORS = [colors.accent, colors.statusHealthy, colors.statusCooling, colors.meta, colors.statusDown, colors.text];

export function seriesColor(index: number): string {
  return SERIES_COLORS[index % SERIES_COLORS.length]!;
}
