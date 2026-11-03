package source

import (
	"context"
	"errors"
	"fmt"
)

// VerifiedLocal is a live OS-checked shared filesystem location. It never
// comes from a raw TV request. The path MUST be revalidated on actual opening.
type VerifiedLocal struct{ path string }

func (v VerifiedLocal) Path() string { return v.path }

type LocalProbe interface {
	Probe(ctx context.Context, metadata Metadata) (VerifiedLocal, bool, error)
}

// Decision is a private Gateway plan. LocalHint and local paths cannot leak
// through normal JSON encoding; never include the plan in a TV event.
type Decision struct {
	Strategy Strategy `json:"strategy"`
	ref      Reference
	meta     Metadata
	local    VerifiedLocal
}

func (d Decision) Reference() Reference                 { return d.ref }
func (d Decision) Metadata() Metadata                   { return d.meta }
func (d Decision) VerifiedLocal() (VerifiedLocal, bool) { return d.local, d.Strategy == LocalDirect }

// Resolver checks the authoritative Storage owner first, then attempts a
// real local-namespace proof. Same host/IP/Tailscale IP is NEVER sufficient.
type Resolver struct {
	Registry Registry
	Local    LocalProbe
}

func (r *Resolver) Resolve(ctx context.Context, ref Reference) (Decision, error) {
	if err := ref.Validate(); err != nil {
		return Decision{}, err
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if r == nil || r.Registry == nil {
		return Decision{}, ErrNotConfigured
	}
	client, err := r.Registry.ForNode(ctx, ref.StorageNodeID)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrSourceOffline, err)
	}
	if client == nil {
		return Decision{}, ErrSourceOffline
	}
	meta, err := client.Stat(ctx, ref.FileID)
	if err != nil {
		if ctx.Err() != nil {
			return Decision{}, ctx.Err()
		}
		return Decision{}, fmt.Errorf("%w: %v", ErrSourceOffline, err)
	}
	if err := meta.Validate(ref); err != nil {
		return Decision{}, err
	}
	plan := Decision{ref: ref, meta: meta}
	if meta.Local != nil && r.Local != nil {
		proof, local, probeErr := r.Local.Probe(ctx, meta)
		if probeErr != nil && (errors.Is(probeErr, ErrUntrustedPath) || ctx.Err() != nil) {
			return Decision{}, probeErr
		}
		if local && probeErr == nil && proof.path != "" {
			plan.Strategy = LocalDirect
			plan.local = proof
			return plan, nil
		}
	}
	if !meta.RangeReadable {
		return Decision{}, ErrRangeUnsupported
	}
	if meta.SizeBytes != nil && *meta.SizeBytes <= PrefetchLimitBytes {
		plan.Strategy = RemotePrefetch
	} else {
		plan.Strategy = RemoteBlockStream
	}
	return plan, nil
}
