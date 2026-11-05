package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LocalReaderFactory opens a verified same-host source from an explicitly
// configured shared-root mapping. It NEVER opens a path supplied by a TV.
// It is not registered in the Gateway by default: HC SS must first authorize
// the file in Resolver.Resolve and produce a LOCAL_DIRECT Decision.
//
// os.OpenRoot confines the actual open to the approved root even if a symlink
// is swapped between metadata resolution and opening the file.
type LocalReaderFactory struct {
	Roots map[string]string
}

func (f LocalReaderFactory) Open(ctx context.Context, plan Decision, byteOffset int64) (Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if byteOffset < 0 || plan.Strategy != LocalDirect || plan.local.path == "" {
		return nil, ErrInvalidReference
	}
	meta := plan.Metadata()
	if meta.Local == nil || meta.SizeBytes == nil || meta.Local.ModifiedUnixNano == nil {
		return nil, ErrInvalidMetadata
	}
	if byteOffset > *meta.SizeBytes {
		return nil, ErrInvalidReference
	}
	rootPath, ok := f.Roots[meta.Local.SharedRootID]
	if !ok || !filepath.IsAbs(rootPath) {
		return nil, ErrUntrustedPath
	}
	name := filepath.FromSlash(meta.Local.RelativePath)
	if name == "." || !filepath.IsLocal(name) {
		return nil, ErrUntrustedPath
	}

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("%w: open root: %v", ErrSourceOffline, err)
	}
	// Open the path RELATIVE TO the anchored directory descriptor. Ordinary
	// os.Open(plan.VerifiedLocal().Path()) would reintroduce a TOCTOU hole.
	file, openErr := root.Open(name)
	_ = root.Close()
	if openErr != nil {
		return nil, fmt.Errorf("%w: file no longer accessible: %v", ErrSourceChanged, openErr)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()

	st, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: file stat: %v", ErrSourceChanged, err)
	}
	if !st.Mode().IsRegular() || st.Size() != *meta.SizeBytes ||
		st.ModTime().UnixNano() != *meta.Local.ModifiedUnixNano {
		return nil, ErrSourceChanged
	}
	// Ensure the opened descriptor is still the originally probed file, not a
	// replacement with the same size and timestamp. Changes fail closed.
	probedStat, err := os.Stat(plan.local.path)
	if err != nil || !os.SameFile(probedStat, st) {
		return nil, ErrSourceChanged
	}
	if _, err = file.Seek(byteOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("%w: seek: %v", ErrSourceOffline, err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	closeOnError = false
	return file, nil
}

// Ensure the factory cannot accidentally be used as a remote source.
var _ ReaderFactory = LocalReaderFactory{}

