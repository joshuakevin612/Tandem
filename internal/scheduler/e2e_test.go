package scheduler

import (
	"context"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"taskq/internal/store"
)

// The Postgres container starts lazily so unit tests need no Docker.
var (
	pgOnce sync.Once
	pgC    *postgres.PostgresContainer
	pgDSN  string
	pgErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pgC != nil {
		_ = testcontainers.TerminateContainer(pgC)
	}
	os.Exit(code)
}

func testDSN(t *testing.T) string {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		pgC, pgErr = postgres.Run(ctx, "postgres:17",
			postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies())
		if pgErr != nil {
			return
		}
		if pgDSN, pgErr = pgC.ConnectionString(ctx, "sslmode=disable"); pgErr != nil {
			return
		}
		pgErr = store.Migrate(pgDSN)
	})
	if pgErr != nil {
		log.Printf("postgres: %v", pgErr)
		t.Fatalf("postgres unavailable: %v", pgErr)
	}
	return pgDSN
}

// countingStore counts FireSchedule calls that actually inserted a job.
type countingStore struct {
	*store.Store
	fired atomic.Int64
}

func (c *countingStore) FireSchedule(ctx context.Context, s store.Schedule, next time.Time) (bool, error) {
	ok, err := c.Store.FireSchedule(ctx, s, next)
	if ok {
		c.fired.Add(1)
	}
	return ok, err
}

const e2eKey int64 = 0x7e57_0001

type instance struct {
	name    string
	cs      *countingStore
	cancel  context.CancelFunc
	done    chan struct{}
	leading atomic.Bool
	terms   atomic.Int32 // times this instance became leader
}

func (i *instance) stop() { i.cancel(); <-i.done }

func newCron(cs *countingStore) *CronRunner {
	return &CronRunner{Store: cs, Interval: 100 * time.Millisecond, BatchSize: 50, Logger: quiet}
}

// startInstance runs a full elector + cron runner on its own store/pool.
func startInstance(t *testing.T, dsn, name string) *instance {
	t.Helper()
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	in := &instance{name: name, cs: &countingStore{Store: st}, cancel: cancel, done: make(chan struct{})}
	el := &Elector{Store: PGLocker{S: st}, Key: e2eKey, RetryInterval: 200 * time.Millisecond, CheckInterval: 100 * time.Millisecond, Logger: quiet}
	cr := newCron(in.cs)
	go func() {
		defer close(in.done)
		defer st.Close()
		_ = el.Run(ctx, func(ctx context.Context) error {
			in.terms.Add(1)
			in.leading.Store(true)
			defer in.leading.Store(false)
			return cr.Run(ctx)
		})
	}()
	t.Cleanup(in.stop)
	return in
}

type jobRow struct {
	key  string
	fire int64 // unix seconds encoded in the idempotency key
	at   time.Time
}

func setup(t *testing.T, dsn string) (*pgx.Conn, string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, `TRUNCATE jobs, schedules CASCADE`); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sc, err := st.CreateSchedule(ctx, store.NewSchedule{
		Name: "every-second", Cron: "@every 1s", Type: "tick", NextFireAt: time.Now().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return conn, sc.ID.String()
}

func readJobs(t *testing.T, conn *pgx.Conn, id string) []jobRow {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		`SELECT idempotency_key, created_at FROM jobs WHERE schedule_id = $1 ORDER BY idempotency_key`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []jobRow
	for rows.Next() {
		var r jobRow
		if err := rows.Scan(&r.key, &r.at); err != nil {
			t.Fatal(err)
		}
		n, err := strconv.ParseInt(r.key[strings.LastIndex(r.key, ":")+1:], 10, 64)
		if err != nil {
			t.Fatalf("bad key %q", r.key)
		}
		r.fire = n
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].fire < out[j].fire })
	return out
}

func assertDistinct(t *testing.T, jobs []jobRow) {
	t.Helper()
	seenKey, seenSec := map[string]bool{}, map[int64]bool{}
	for _, j := range jobs {
		if seenKey[j.key] || seenSec[j.fire] {
			t.Fatalf("duplicate fire: %s (second %d)", j.key, j.fire)
		}
		seenKey[j.key], seenSec[j.fire] = true, true
	}
}

func waitUntil(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func leaderOf(a, b *instance) *instance {
	switch {
	case a.leading.Load() && !b.leading.Load():
		return a
	case b.leading.Load() && !a.leading.Load():
		return b
	}
	return nil
}

func TestE2EFailover(t *testing.T) {
	for _, mode := range []string{"cancel", "terminate-backend"} {
		t.Run(mode, func(t *testing.T) {
			dsn := testDSN(t)
			ctrl, id := setup(t, dsn)
			start := time.Now()
			a, b := startInstance(t, dsn, "a"), startInstance(t, dsn, "b")

			waitUntil(t, "a leader", 5*time.Second, func() bool { return leaderOf(a, b) != nil })
			time.Sleep(4 * time.Second)
			old := leaderOf(a, b)
			if old == nil {
				t.Fatal("no single leader after 4s")
			}
			other := a
			if old == a {
				other = b
			}
			before := len(readJobs(t, ctrl, id))
			if before < 2 {
				t.Fatalf("only %d jobs fired in first 4s", before)
			}

			termsBefore := old.terms.Load() + other.terms.Load()
			deposed := time.Now()
			switch mode {
			case "cancel":
				old.stop()
			case "terminate-backend":
				// Kill the backend that holds the advisory lock, from a third connection.
				var killed int
				err := ctrl.QueryRow(context.Background(), `
					SELECT count(*) FROM (
						SELECT pg_terminate_backend(pid) FROM pg_locks
						WHERE locktype = 'advisory' AND granted
						  AND ((classid::bigint << 32) | objid::bigint) = $1) k`, e2eKey).Scan(&killed)
				if err != nil || killed != 1 {
					t.Fatalf("terminate: killed=%d err=%v", killed, err)
				}
			}

			if mode == "cancel" {
				waitUntil(t, "other instance to lead", 6*time.Second, func() bool { return other.leading.Load() })
			} else {
				waitUntil(t, "a new leadership term", 6*time.Second, func() bool {
					return old.terms.Load()+other.terms.Load() > termsBefore && leaderOf(a, b) != nil
				})
			}
			newLeader := leaderOf(a, b)
			t.Logf("mode=%s old=%s new=%s takeover=%v", mode, old.name, newLeader.name, time.Since(deposed).Round(time.Millisecond))
			if mode == "cancel" && newLeader != other {
				t.Fatalf("leader after cancel = %s, want %s", newLeader.name, other.name)
			}

			time.Sleep(4 * time.Second)
			elapsed := time.Since(start)
			jobs := readJobs(t, ctrl, id)
			assertDistinct(t, jobs)

			// Firing resumed within ~4s of the failover.
			var after []jobRow
			for _, j := range jobs {
				if j.at.After(deposed) {
					after = append(after, j)
				}
			}
			if len(after) == 0 {
				t.Fatal("no fires after failover")
			}
			resume := after[0].at.Sub(deposed)
			maxGap := int64(0)
			for i := 1; i < len(jobs); i++ {
				if g := jobs[i].fire - jobs[i-1].fire; g > maxGap {
					maxGap = g
				}
			}
			t.Logf("jobs=%d elapsed=%v first-fire-after-failover=%v max-fire-gap=%ds fired(a)=%d fired(b)=%d",
				len(jobs), elapsed.Round(100*time.Millisecond), resume.Round(time.Millisecond), maxGap, a.cs.fired.Load(), b.cs.fired.Load())
			if resume > 4*time.Second {
				t.Fatalf("firing resumed %v after failover, want <= 4s", resume)
			}
			if want := int(elapsed.Seconds()); len(jobs) < want-2 || len(jobs) > want+2 {
				t.Fatalf("jobs = %d, want %d +/- 2", len(jobs), want)
			}
			if total := a.cs.fired.Load() + b.cs.fired.Load(); int(total) != len(jobs) {
				t.Fatalf("instances report %d fires but %d jobs exist", total, len(jobs))
			}
		})
	}
}

// With no election both cron runners are live at once; the store's
// compare-and-swap alone must prevent duplicate fires.
func TestE2ENoElectionNoDuplicateFires(t *testing.T) {
	dsn := testDSN(t)
	ctrl, id := setup(t, dsn)
	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var stores []*countingStore
	for i := 0; i < 2; i++ {
		st, err := store.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		cs := &countingStore{Store: st}
		stores = append(stores, cs)
		cr := newCron(cs)
		wg.Add(1)
		go func() { defer wg.Done(); defer st.Close(); _ = cr.Run(ctx) }()
	}
	time.Sleep(4 * time.Second)
	cancel()
	wg.Wait()
	elapsed := time.Since(start)

	jobs := readJobs(t, ctrl, id)
	assertDistinct(t, jobs)
	t.Logf("jobs=%d elapsed=%v fired(a)=%d fired(b)=%d", len(jobs), elapsed.Round(100*time.Millisecond), stores[0].fired.Load(), stores[1].fired.Load())
	if want := int(elapsed.Seconds()); len(jobs) < want-2 || len(jobs) > want+2 {
		t.Fatalf("jobs = %d, want %d +/- 2", len(jobs), want)
	}
	if total := stores[0].fired.Load() + stores[1].fired.Load(); int(total) != len(jobs) {
		t.Fatalf("instances report %d fires but %d jobs exist", total, len(jobs))
	}
}
