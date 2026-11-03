package source

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// RootProbe maps HC SS's shared root identifiers to Gateway-admin configured
// actual local filesystem roots. No network address or absolute hint is used.
// The final file is OPENED and stat'ed to prove OS read access and matching
// size/modification version; a missing mapping means remote fallback.
type RootProbe struct{ Roots map[string]string }

func within(root, file string) bool {
	rel, err := filepath.Rel(root, file)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "."
}

func (p RootProbe) Probe(ctx context.Context, m Metadata) (VerifiedLocal, bool, error) {
	if err := ctx.Err(); err != nil {
		return VerifiedLocal{}, false, err
	}
	if m.Local == nil {
		return VerifiedLocal{}, false, nil
	}
	root, present := p.Roots[m.Local.SharedRootID]
	if !present {
		return VerifiedLocal{}, false, nil
	}
	// Exact size and timestamp from the owner are needed for the local proof.
	if m.SizeBytes == nil || m.Local.ModifiedUnixNano == nil {
		return VerifiedLocal{}, false, nil
	}
	if root == "" || !filepath.IsAbs(root) {
		return VerifiedLocal{}, false, ErrUntrustedPath
	}
	name := filepath.FromSlash(m.Local.RelativePath)
	if name == "." || !filepath.IsLocal(name) {
		return VerifiedLocal{}, false, ErrUntrustedPath
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return VerifiedLocal{}, false, nil
	}
	rootInfo, err := os.Stat(canonicalRoot)
	if err != nil || !rootInfo.IsDir() {
		return VerifiedLocal{}, false, nil
	}
	absolute, err := filepath.EvalSymlinks(filepath.Join(canonicalRoot, name))
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return VerifiedLocal{}, false, nil
	}
	if err != nil {
		return VerifiedLocal{}, false, nil
	}
	if !within(canonicalRoot, absolute) {
		return VerifiedLocal{}, false, ErrUntrustedPath
	}
	file, err := os.Open(absolute)
	if err != nil {
		return VerifiedLocal{}, false, nil
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() != *m.SizeBytes || stat.ModTime().UnixNano() != *m.Local.ModifiedUnixNano {
		return VerifiedLocal{}, false, nil
	}
	// Catch replacement between EvalSymlinks and Open; actual readers must
	// perform the same check again, so this is a selection proof, not a lease.
	byPath, err := os.Stat(absolute)
	if err != nil || !os.SameFile(stat, byPath) {
		return VerifiedLocal{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return VerifiedLocal{}, false, err
	}
	return VerifiedLocal{path: absolute}, true, nil
}
