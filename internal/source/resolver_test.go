package source

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeClient struct {
	metadata Metadata
	err      error
	calls    int
}

func (c *fakeClient) Stat(_ context.Context, id string) (Metadata, error) {
	c.calls++
	return c.metadata, c.err
}
func (c *fakeClient) OpenRange(context.Context, RangeRequest) (io.ReadCloser, RangeReply, error) {
	return nil, RangeReply{}, ErrNotConfigured
}

type fakeRegistry struct {
	name   string
	client Client
	err    error
}

func (r fakeRegistry) ForNode(_ context.Context, n string) (Client, error) {
	if n != r.name {
		return nil, ErrSourceOffline
	}
	return r.client, r.err
}
func ptr(v int64) *int64 { return &v }
func fixture(size *int64) (Reference, Metadata, *fakeClient, *Resolver) {
	ref := Reference{StorageNodeID: "disk-node", FileID: "movie-1"}
	meta := Metadata{OwnerNodeID: ref.StorageNodeID, FileID: ref.FileID, Version: "rev-002", SizeBytes: size, RangeReadable: true}
	client := &fakeClient{metadata: meta}
	resolver := &Resolver{Registry: fakeRegistry{name: ref.StorageNodeID, client: client}}
	return ref, meta, client, resolver
}
func TestRemoteThresholdsAndUnknownSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		size *int64
		want Strategy
	}{
		{"empty", ptr(0), RemotePrefetch}, {"one", ptr(1), RemotePrefetch},
		{"one GiB", ptr(PrefetchLimitBytes), RemotePrefetch},
		{"one GiB plus one", ptr(PrefetchLimitBytes + 1), RemoteBlockStream},
		{"unknown", nil, RemoteBlockStream},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, _, client, resolver := fixture(tc.size)
			got, err := resolver.Resolve(context.Background(), ref)
			if err != nil || got.Strategy != tc.want || client.calls != 1 {
				t.Fatalf("%+v %v %d", got, err, client.calls)
			}
		})
	}
}
func TestAuthenticatedOwnerRequired(t *testing.T) {
	ref, _, client, resolver := fixture(ptr(120))
	bad := ref
	bad.StorageNodeID = "wrong"
	if _, err := resolver.Resolve(context.Background(), bad); !errors.Is(err, ErrSourceOffline) {
		t.Fatal(err)
	}
	client.metadata.OwnerNodeID = "other-node"
	if _, err := resolver.Resolve(context.Background(), ref); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatal(err)
	}
	client.metadata.OwnerNodeID = ref.StorageNodeID
	client.metadata.SizeBytes = ptr(-1)
	if _, err := resolver.Resolve(context.Background(), ref); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatal(err)
	}
	client.metadata.SizeBytes = ptr(3)
	client.metadata.RangeReadable = false
	if _, err := resolver.Resolve(context.Background(), ref); !errors.Is(err, ErrRangeUnsupported) {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), Reference{StorageNodeID: "/", FileID: ""}); !errors.Is(err, ErrInvalidReference) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(ctx, ref); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestLocalVerifiedReadOnlyFilesystemPath(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "movies", "test.mkv")
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte("movie-data"), 0600); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	ref, meta, client, resolver := fixture(ptr(stat.Size()))
	meta.Local = &LocalHint{SharedRootID: "media-drive-1", RelativePath: "movies/test.mkv", ModifiedUnixNano: ptr(stat.ModTime().UnixNano())}
	client.metadata = meta
	resolver.Local = RootProbe{Roots: map[string]string{"media-drive-1": root}}
	got, err := resolver.Resolve(context.Background(), ref)
	if err != nil || got.Strategy != LocalDirect {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	path, ok := got.VerifiedLocal()
	if !ok || path.Path() != name {
		t.Fatalf("verified location: %v %v", path, ok)
	}
	// Filename/address alone is never proof of locality; missing mapping falls back.
	resolver.Local = RootProbe{Roots: map[string]string{}}
	got, err = resolver.Resolve(context.Background(), ref)
	if err != nil || got.Strategy != RemotePrefetch {
		t.Fatalf("missing mount %v %v", got.Strategy, err)
	}
	// Incompatible file versions or permissions do not produce LOCAL_DIRECT.
	resolver.Local = RootProbe{Roots: map[string]string{"media-drive-1": root}}
	client.metadata.Local.ModifiedUnixNano = ptr(stat.ModTime().UnixNano() + 1)
	got, err = resolver.Resolve(context.Background(), ref)
	if err != nil || got.Strategy != RemotePrefetch {
		t.Fatalf("mismatching revision %v %v", got.Strategy, err)
	}
	client.metadata.Local.ModifiedUnixNano = ptr(stat.ModTime().UnixNano())
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	got, err = resolver.Resolve(context.Background(), ref)
	if err != nil || got.Strategy != RemotePrefetch {
		t.Fatalf("file missing %v %v", got.Strategy, err)
	}
}
func TestTraversalAndSymlinkEscapeRejected(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	file := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, meta, client, resolver := fixture(ptr(1))
	meta.Local = &LocalHint{SharedRootID: "root", RelativePath: "../secret.txt", ModifiedUnixNano: ptr(0)}
	client.metadata = meta
	resolver.Local = RootProbe{Roots: map[string]string{"root": root}}
	if _, err := resolver.Resolve(context.Background(), ref); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatal("traversal", err)
	}
	meta.Local.RelativePath = "linked/secret.txt"
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	client.metadata = meta
	if _, err := resolver.Resolve(context.Background(), ref); !errors.Is(err, ErrUntrustedPath) {
		t.Fatal("symlink escape", err)
	}
}
func TestRangeBoundsAndVersion(t *testing.T) {
	good := RangeRequest{FileID: "x", Version: "revision", Offset: 0, Length: MaxRangeBytes}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, q := range []RangeRequest{{FileID: "x", Version: "revision", Length: 0}, {FileID: "x", Version: "revision", Offset: -1, Length: 1}, {FileID: "x", Version: "revision", Length: MaxRangeBytes + 1}} {
		if q.Validate() == nil {
			t.Fatalf("accepted %+v", q)
		}
	}
	if err := (RangeReply{Offset: 0, Length: 1, Version: "revision"}).Validate(good); err != nil {
		t.Fatal(err)
	}
	if err := (RangeReply{Offset: 0, Length: 1, Version: "old"}).Validate(good); !errors.Is(err, ErrSourceChanged) {
		t.Fatal(err)
	}
	if err := (RangeReply{Offset: 0, Length: MaxRangeBytes + 1, Version: "revision"}).Validate(good); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatal(err)
	}
}
func TestPlanGenerationFenceAndReaderSelection(t *testing.T) {
	ref, _, _, r := fixture(ptr(18))
	co := Coordinator{Resolver: r, Plans: NewPlans()}
	a := Token{RoomID: "r", PlaybackID: "p1", Generation: 1}
	decision, err := co.Prepare(context.Background(), a, ref)
	if err != nil || decision.Strategy != RemotePrefetch {
		t.Fatal(decision, err)
	}
	if err := co.Plans.Save(a, decision); err != nil {
		t.Fatal("idempotent plan", err)
	}
	changed := decision
	changed.meta.Version = "changed"
	if err := co.Plans.Save(a, changed); !errors.Is(err, ErrSourceChanged) {
		t.Fatal("conflicting plan", err)
	}
	b := Token{RoomID: "r", PlaybackID: "p2", Generation: 2}
	if _, err := co.Prepare(context.Background(), b, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Plans.Get(a); !errors.Is(err, ErrStalePlan) {
		t.Fatal(err)
	}
	if _, err := co.Prepare(context.Background(), a, ref); !errors.Is(err, ErrStalePlan) {
		t.Fatal(err)
	}
	if _, err := OpenFromPlan(context.Background(), b, 0, co.Plans, nil); !errors.Is(err, ErrNotConfigured) {
		t.Fatal(err)
	}
	factory := fakeFactory{}
	s, err := OpenFromPlan(context.Background(), b, 1, co.Plans, map[Strategy]ReaderFactory{RemotePrefetch: factory})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(s)
	if err != nil || string(payload) != "ok" {
		t.Fatal(err, string(payload))
	}
	_ = s.Close()
	co.Remove("r")
	if _, err := co.Plans.Get(b); !errors.Is(err, ErrStalePlan) {
		t.Fatal(err)
	}
	if _, err := OpenFromPlan(context.Background(), b, -1, co.Plans, nil); !errors.Is(err, ErrInvalidReference) {
		t.Fatal(err)
	}
	co.Reset()
}

type fakeFactory struct{}

func (fakeFactory) Open(ctx context.Context, plan Decision, off int64) (Reader, error) {
	if off != 1 || plan.Strategy != RemotePrefetch {
		return nil, errors.New("wrong plan")
	}
	return io.NopCloser(strings.NewReader("ok")), nil
}
