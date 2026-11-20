// Package web serves the embedded static web console under /ui/.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed static
var staticFS embed.FS

const csp = "default-src 'self'; style-src 'self' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; script-src 'self'; frame-ancestors 'none'"

// Handler serves the embedded files rooted at static/ under the /ui/ prefix.
// Unknown extension-less paths fall back to index.html; unknown paths with an
// extension are 404.
func Handler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/ui"))
		name = strings.TrimPrefix(name, "/")
		if name == "" {
			name = "index.html"
		}
		if st, err := fs.Stat(sub, name); err != nil || st.IsDir() {
			if path.Ext(name) != "" {
				http.NotFound(w, r)
				return
			}
			name = "index.html"
		}
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", csp)
		// Every file, not just index.html: embedded files carry no modtime, so
		// without this browsers cache CSS/JS heuristically and keep serving the
		// old console after a deploy.
		h.Set("Cache-Control", "no-cache")
		// Serve via FileServer with a rewritten path; index.html is requested
		// as "/" so FileServer does not redirect "/index.html" to "./".
		r2 := r.Clone(r.Context())
		if name == "index.html" {
			r2.URL.Path = "/"
		} else {
			r2.URL.Path = "/" + name
		}
		files.ServeHTTP(w, r2)
	})
}
