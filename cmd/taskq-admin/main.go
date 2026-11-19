// Command taskq-admin manages API keys. It uses DATABASE_URL like the other binaries.
//
//	taskq-admin create-key -tenant X -name Y   (prints the key once)
//	taskq-admin revoke-key -id UUID
//	taskq-admin list-keys  -tenant X
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"taskq/internal/config"
	"taskq/internal/store"
)

const prefixLen = 8

// newKey returns "tq_" + base64url(32 random bytes).
func newKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "tq_" + base64.RawURLEncoding.EncodeToString(b), nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	tenant := fs.String("tenant", "", "tenant id")
	name := fs.String("name", "", "key name")
	id := fs.String("id", "", "key id")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := store.Open(ctx, config.Load().DatabaseURL)
	if err != nil {
		fail(err)
	}
	defer st.Close()

	switch cmd {
	case "create-key":
		if *tenant == "" {
			fail(errors.New("-tenant is required"))
		}
		key, err := newKey()
		if err != nil {
			fail(err)
		}
		h := sha256.Sum256([]byte(key))
		k, err := st.CreateAPIKey(ctx, *tenant, *name, key[:prefixLen], h[:])
		if err != nil {
			fail(err)
		}
		fmt.Printf("id:     %s\ntenant: %s\nkey:    %s\n\nStore this key now; it cannot be shown again.\n", k.ID, k.TenantID, key)
	case "revoke-key":
		kid, err := uuid.Parse(*id)
		if err != nil {
			fail(errors.New("-id must be a UUID"))
		}
		if err := st.RevokeAPIKey(ctx, kid); err != nil {
			fail(err)
		}
		fmt.Println("revoked", kid)
	case "list-keys":
		if *tenant == "" {
			fail(errors.New("-tenant is required"))
		}
		keys, err := st.ListAPIKeys(ctx, *tenant)
		if err != nil {
			fail(err)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tPREFIX\tNAME\tCREATED\tREVOKED")
		for _, k := range keys {
			rev := "-"
			if k.RevokedAt != nil {
				rev = k.RevokedAt.Format(time.RFC3339)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", k.ID, k.Prefix, k.Name, k.CreatedAt.Format(time.RFC3339), rev)
		}
		_ = w.Flush()
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: taskq-admin create-key -tenant X [-name Y] | revoke-key -id UUID | list-keys -tenant X")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
