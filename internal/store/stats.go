package store

import "context"

// QueueStat is the number of jobs in one (queue, status) group. For queued
// groups OldestRunnableAge is how long the oldest already-runnable job has
// been waiting, in seconds (0 when none is runnable yet).
type QueueStat struct {
	Queue             string
	Status            string
	Count             int64
	OldestRunnableAge float64
}

// QueueStats returns job counts per (queue, status) in a single aggregate
// query. The age is computed in SQL against the database clock so it is
// immune to clock skew between the caller and Postgres.
func (s *Store) QueueStats(ctx context.Context) ([]QueueStat, error) {
	const q = `
SELECT queue, status, count(*),
	COALESCE(EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE status = 'queued' AND run_at <= now())), 0)::float8
FROM jobs GROUP BY 1, 2`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueStat
	for rows.Next() {
		var st QueueStat
		if err := rows.Scan(&st.Queue, &st.Status, &st.Count, &st.OldestRunnableAge); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}
