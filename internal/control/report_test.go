package control

import (
	"encoding/json"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
)

func TestMediaExecutionIsNotAttach(t *testing.T) {
	base := directws.Update{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 3, Phase: "READY"}
	action, id, data := directUpdateCommand(base)
	if action != "media.room.attach" || id == "" {
		t.Fatalf("attach %q %q", action, id)
	}
	raw, _ := json.Marshal(data)
	if string(raw) == "" {
		t.Fatal("missing attachment")
	}
	base.Phase = "STREAMING"
	base.Sequence = 1
	action, id, data = directUpdateCommand(base)
	if action != "media.room.report" || id == "" {
		t.Fatalf("execution %q %q", action, id)
	}
	raw, _ = json.Marshal(data)
	var q map[string]any
	if err := json.Unmarshal(raw, &q); err != nil {
		t.Fatal(err)
	}
	if q["participant"] != "gateway" || q["report_sequence"] != float64(1) || q["connection_epoch"] != float64(3) {
		t.Fatalf("report %+v", q)
	}
}
