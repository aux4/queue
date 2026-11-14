package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// tableName prints the SQL table identifier --storage repository uses for a
// queue's messages (args: queue name). Every pre-existing underscore-only
// queue name maps to exactly the same "<name>_messages" it always has
// (backward compatible, no re-migration needed for anyone already running
// repository storage). A name containing any other character (notably a
// dash or a dot -- both natural, common queue names, and both fine under
// --storage memory, which never uses the name as a SQL identifier) is
// sanitized into a safe identifier and disambiguated with a short hash of
// the original name, so it never collides with another name that happens
// to sanitize to the same text.
func tableName(args []string) {
	name := getArg(args, 0, "")
	fmt.Print(safeTableName(name))
}

// safeTableName returns the SQL table identifier for a queue's messages.
func safeTableName(name string) string {
	if isSafeSQLIdentifier(name) {
		return name + "_messages"
	}
	return sanitizeSQLIdentifier(name) + "_" + shortHash(name) + "_messages"
}

// isSafeSQLIdentifier reports whether s is already a plain SQL identifier:
// starts with a letter or underscore, and contains only letters, digits, and
// underscores.
func isSafeSQLIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		isAlpha := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
		isDigit := r >= '0' && r <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit {
			return false
		}
	}
	return true
}

// sanitizeSQLIdentifier replaces every character that is not a letter,
// digit, or underscore with an underscore, and prefixes an underscore if the
// result would not start with a letter or underscore.
func sanitizeSQLIdentifier(s string) string {
	var b strings.Builder
	for i, r := range s {
		isAlpha := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
		isDigit := r >= '0' && r <= '9'
		if i == 0 && !isAlpha {
			b.WriteByte('_')
		}
		if isAlpha || isDigit {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

// shortHash returns the first 8 hex characters of the SHA-256 digest of s,
// used to disambiguate two different queue names that happen to sanitize to
// the same text (e.g. "a-b" and "a.b" both sanitize to "a_b").
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:8]
}
