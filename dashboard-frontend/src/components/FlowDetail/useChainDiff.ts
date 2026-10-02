/**
 * useChainDiff — the inspector's "vs previous in chain" comparison.
 *
 * Fetches the FULL reassembled inbound bodies of this flow and its chain predecessor from the
 * durable history API (the content store joins skeleton + items), and diffs them structurally.
 * Both queries are enabled only while the Chain tab is open, so the default inspector costs
 * nothing extra. A flow without a predecessor (`new_chain`, or lineage not computed) yields
 * `predecessorId: null` and no fetch.
 */
import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConnection, queryKeys } from '../../api/connection';
import { diffLayers, type DiffMap } from './diff';

export interface ChainDiff {
  predecessorId: string | null;
  /** The predecessor's reassembled inbound body (undefined while loading / unavailable). */
  previous: unknown;
  /** This flow's reassembled inbound body. */
  current: unknown;
  diff: DiffMap;
  loading: boolean;
  /** A load failure message (503 = durable history disabled), or null. */
  error: string | null;
}

export function useChainDiff(apiCallId: string, predecessorId: string | null | undefined, enabled: boolean): ChainDiff {
  const { client } = getConnection();
  const previousId = predecessorId ?? null;
  const currentQuery = useQuery({
    queryKey: queryKeys.requestBody(apiCallId, 'client_in'),
    queryFn: () => client.historyRequestBody(apiCallId, 'client_in'),
    enabled,
    retry: false,
  });
  const previousQuery = useQuery({
    queryKey: previousId ? queryKeys.requestBody(previousId, 'client_in') : ['history', 'requests', '__none__'],
    queryFn: () => client.historyRequestBody(previousId as string, 'client_in'),
    enabled: enabled && !!previousId,
    retry: false,
  });
  const diff = useMemo(
    () => diffLayers(previousQuery.data, currentQuery.data),
    [previousQuery.data, currentQuery.data],
  );
  const error = currentQuery.error ?? previousQuery.error;
  return {
    predecessorId: previousId,
    previous: previousQuery.data,
    current: currentQuery.data,
    diff,
    loading: enabled && (currentQuery.isPending || (!!previousId && previousQuery.isPending)),
    error: error ? (error instanceof Error ? error.message : String(error)) : null,
  };
}
