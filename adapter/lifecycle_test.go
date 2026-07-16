package adapter

import "testing"

func TestStartStagesThrough(t *testing.T) {
	for stage := StartStateInitialize; stage <= StartStateStarted; stage++ {
		stages := StartStagesThrough(stage)
		if len(stages) != int(stage)+1 || stages[len(stages)-1] != stage {
			t.Fatalf("unexpected stages through %s: %v", stage.String(), stages)
		}
	}
}
