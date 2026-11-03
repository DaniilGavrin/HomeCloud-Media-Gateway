package source

import (
	"reflect"
	"sync"
)

// Token fences a chosen storage strategy by room/playback/generation.
// A late old-generation result cannot become the next playback's source.
type Token struct {
	RoomID     string
	PlaybackID string
	Generation uint32
}

type entry struct {
	token Token
	plan  Decision
}

type Plans struct {
	mu    sync.Mutex
	rooms map[string]entry
}

func NewPlans() *Plans { return &Plans{rooms: make(map[string]entry)} }
func (p *Plans) Save(tok Token, plan Decision) error {
	if p == nil || tok.RoomID == "" || tok.PlaybackID == "" || tok.Generation == 0 || plan.Strategy == "" {
		return ErrInvalidReference
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.rooms[tok.RoomID]; ok {
		if old.token.Generation > tok.Generation || (old.token.Generation == tok.Generation && old.token.PlaybackID != tok.PlaybackID) {
			return ErrStalePlan
		}
		if old.token == tok {
			if old.plan.Strategy != plan.Strategy || old.plan.local != plan.local || !reflect.DeepEqual(old.plan.meta, plan.meta) {
				return ErrSourceChanged
			}
			return nil
		}
	}
	p.rooms[tok.RoomID] = entry{token: tok, plan: plan}
	return nil
}
func (p *Plans) Get(tok Token) (Decision, error) {
	if p == nil {
		return Decision{}, ErrNotConfigured
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	old, ok := p.rooms[tok.RoomID]
	if !ok || old.token != tok {
		return Decision{}, ErrStalePlan
	}
	return old.plan, nil
}
func (p *Plans) Remove(roomID string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.rooms, roomID)
	p.mu.Unlock()
}
func (p *Plans) Reset() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.rooms = make(map[string]entry)
	p.mu.Unlock()
}
