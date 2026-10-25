package tasker

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/net/idna"
)

// BasicAuth contains deployment credentials, never persisted in task records.
type BasicAuth struct {
	Username string
	Password string
}

func (a BasicAuth) Validate() error {
	if strings.TrimSpace(a.Username) == "" || strings.Contains(a.Username, ":") || strings.ContainsFunc(a.Username, unicode.IsControl) {
		return fmt.Errorf("basic auth username must be nonempty and contain no colon or control characters")
	}

	if a.Password == "" || strings.ContainsFunc(a.Password, unicode.IsControl) {
		return fmt.Errorf("basic auth password must be nonempty and contain no control characters")
	}

	return nil
}

// Origin normalizes an HTTP URL to scheme, hostname and effective port.
func Origin(u *url.URL) (string, error) {
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "http" && scheme != "https") || u.Hostname() == "" || u.User != nil || u.Opaque != "" {
		return "", ErrInvalidTaskURL
	}

	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", ErrInvalidTaskURL
	}

	host := u.Hostname()
	if strings.Contains(host, ":") {
		address, err := netip.ParseAddr(host)
		if err != nil {
			return "", ErrInvalidTaskURL
		}

		// Normalize the address without changing the case-sensitive zone.
		host = address.String()
	} else {
		// Match net/http's IDNA lookup profile and ASCII fast path. Convert
		// before lowercasing: Unicode case folding can change the destination.
		if strings.ContainsFunc(host, func(r rune) bool { return r >= unicode.MaxASCII+1 }) {
			host, err = idna.Lookup.ToASCII(host)
			if err != nil {
				return "", ErrInvalidTaskURL
			}
		}

		host = strings.ToLower(host)
	}

	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(n)), nil
}

func basicAuthHandler(next http.Handler, auth *BasicAuth) http.Handler {
	if auth == nil {
		return next
	}

	expectedUser := sha256.Sum256([]byte(auth.Username))
	expectedPassword := sha256.Sum256([]byte(auth.Password))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		userHash := sha256.Sum256([]byte(username))
		passwordHash := sha256.Sum256([]byte(password))

		userMatch := subtle.ConstantTimeCompare(userHash[:], expectedUser[:])
		passwordMatch := subtle.ConstantTimeCompare(passwordHash[:], expectedPassword[:])
		if !ok || userMatch&passwordMatch != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="tasker", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	})
}
