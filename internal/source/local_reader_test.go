package source

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func makeLocalPlan(t *testing.T, root, rel string) Decision {
	t.Helper()
	file := filepath.Join(root, filepath.FromSlash(rel))
	st, err := os.Stat(file)
	if err != nil { t.Fatal(err) }
	size := st.Size()
	mtime := st.ModTime().UnixNano()
	ref := Reference{StorageNodeID: "storage-a", FileID: "opaque-file-a"}
	meta := Metadata{
		OwnerNodeID: ref.StorageNodeID, FileID: ref.FileID, Version: "revision-a",
		SizeBytes: &size, ContentType: "video/mp4", RangeReadable: true,
		Local: &LocalHint{SharedRootID: "shared-a", RelativePath: rel, ModifiedUnixNano: &mtime},
	}
	proof, ok, err := (RootProbe{Roots: map[string]string{"shared-a": root}}).Probe(context.Background(), meta)
	if err != nil || !ok {
		t.Fatalf("cannot validate fixture: proof=%v ok=%v err=%v", proof, ok, err)
	}
	return Decision{Strategy: LocalDirect, ref: ref, meta: meta, local: proof}
}

func TestLocalReaderAtOffsetAndEOF(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "music"), 0700); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(root, "music", "track.m4a"), []byte("0123456789"), 0600); err != nil { t.Fatal(err) }
	plan := makeLocalPlan(t, root, "music/track.m4a")
	plan.meta.ContentType = "audio/mp4" // Reader is media-type agnostic.
	factory := LocalReaderFactory{Roots: map[string]string{"shared-a": root}}
	reader, err := factory.Open(context.Background(), plan, 3)
	if err != nil { t.Fatal(err) }
	data, err := io.ReadAll(reader)
	if closeErr := reader.Close(); closeErr != nil { t.Fatal(closeErr) }
	if err != nil || string(data) != "3456789" { t.Fatalf("read %q: %v", data, err) }

	reader, err = factory.Open(context.Background(), plan, 10)
	if err != nil { t.Fatal(err) }
	data, err = io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || len(data) != 0 { t.Fatalf("expected EOF, got %q: %v", data, err) }
	if _, err := factory.Open(context.Background(), plan, 11); !errors.Is(err, ErrInvalidReference) { t.Fatalf("past EOF: %v", err) }
}

func TestLocalReaderRejectsModifiedOrReplacedFile(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "track.mp3")
	if err := os.WriteFile(file, []byte("old"), 0600); err != nil { t.Fatal(err) }
	plan := makeLocalPlan(t, root, "track.mp3")
	factory := LocalReaderFactory{Roots: map[string]string{"shared-a": root}}
	if err := os.WriteFile(file, []byte("different-length"), 0600); err != nil { t.Fatal(err) }
	if _, err := factory.Open(context.Background(), plan, 0); !errors.Is(err, ErrSourceChanged) { t.Fatalf("changed source: %v", err) }
}

func TestLocalReaderRejectsSymlinkSwapOutsideRoot(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	file := filepath.Join(root, "track.mp3")
	if err := os.WriteFile(file, []byte("audio"), 0600); err != nil { t.Fatal(err) }
	plan := makeLocalPlan(t, root, "track.mp3")
	secret := filepath.Join(outside, "secret.mp3")
	if err := os.WriteFile(secret, []byte("audio"), 0600); err != nil { t.Fatal(err) }
	if err := os.Remove(file); err != nil { t.Fatal(err) }
	if err := os.Symlink(secret, file); err != nil { t.Skipf("symlinks unavailable: %v", err) }
	_, err := (LocalReaderFactory{Roots: map[string]string{"shared-a": root}}).Open(context.Background(), plan, 0)
	if !errors.Is(err, ErrSourceChanged) { t.Fatalf("symlink escape was not rejected: %v", err) }
}

func TestLocalReaderRejectsStalePlanAndCanceledContext(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "v.mp4")
	if err := os.WriteFile(file, []byte("data"), 0600); err != nil { t.Fatal(err) }
	plan := makeLocalPlan(t, root, "v.mp4")
	factory := LocalReaderFactory{Roots: map[string]string{"shared-a": root}}
	stale := plan
	stale.Strategy = RemotePrefetch
	if _, err := factory.Open(context.Background(), stale, 0); !errors.Is(err, ErrInvalidReference) { t.Fatal(err) }
	if _, err := (LocalReaderFactory{}).Open(context.Background(), plan, 0); !errors.Is(err, ErrUntrustedPath) { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := factory.Open(ctx, plan, 0); !errors.Is(err, context.Canceled) { t.Fatal(err) }
}
