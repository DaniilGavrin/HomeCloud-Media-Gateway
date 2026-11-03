package source

import (
	"context"
	"errors"
)

// Coordinator is attached only after an authenticated HC SS connector exists.
// Gateway offers are first checked against HC SS metadata; successful plans
// are stored under the generation fence and read by future KAN-57..59 readers.
type Coordinator struct {
	Resolver *Resolver
	Plans    *Plans
}

func (c *Coordinator) Prepare(ctx context.Context, token Token, ref Reference) (Decision, error) {
	if c == nil || c.Resolver == nil || c.Plans == nil {
		return Decision{}, ErrNotConfigured
	}
	if token.RoomID == "" || token.PlaybackID == "" || token.Generation == 0 {
		return Decision{}, ErrInvalidReference
	}
	plan, err := c.Resolver.Resolve(ctx, ref)
	if err != nil {
		return Decision{}, err
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if err := c.Plans.Save(token, plan); err != nil {
		return Decision{}, err
	}
	return plan, nil
}
func (c *Coordinator) Remove(roomID string) {
	if c != nil {
		c.Plans.Remove(roomID)
	}
}
func (c *Coordinator) Reset() {
	if c != nil {
		c.Plans.Reset()
	}
}

// OpenFromPlan chooses the future KAN-57..59 reader, but never pretends an
// uninstalled transport works. The plan by itself is not an open stream.
func OpenFromPlan(ctx context.Context, token Token, byteOffset int64, plans *Plans, factories map[Strategy]ReaderFactory) (Reader, error) {
	if byteOffset < 0 {
		return nil, ErrInvalidReference
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, err := plans.Get(token)
	if err != nil {
		return nil, err
	}
	factory := factories[plan.Strategy]
	if factory == nil {
		return nil, ErrNotConfigured
	}
	reader, err := factory.Open(ctx, plan, byteOffset)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("SOURCE_READER_RETURNED_NIL")
	}
	return reader, nil
}
