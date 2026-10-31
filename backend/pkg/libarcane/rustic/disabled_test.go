package rustic

import "testing"

func TestDisabledFromEnvReadsEnv(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", "yes", " On "} {
		t.Setenv(EnvDisabled, value)
		if !DisabledFromEnv() {
			t.Fatalf("%s=%q should disable the backup engine", EnvDisabled, value)
		}
	}
	for _, value := range []string{"", "0", "false", "off", "nope"} {
		t.Setenv(EnvDisabled, value)
		if DisabledFromEnv() {
			t.Fatalf("%s=%q should leave the backup engine enabled", EnvDisabled, value)
		}
	}
}
