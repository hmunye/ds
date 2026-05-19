package messenger

import (
	"testing"
)

func TestParseMessageType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "valid echo message",
			input: `{"src":"c1","dest":"n1","body":{"type":"echo","msg_id":1}}`,
			want:  "echo",
		},
		{
			name:    "missing type",
			input:   `{"src":"c1","dest":"n1","body":{"msg_id":1}}`,
			wantErr: true,
		},
		{
			name:    "malformed JSON",
			input:   `{"src":"c1" "dest":"n1","body":{"type":"echo","msg_id":1}}`,
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()
			got, err := parseMessageType([]byte(tt.input))
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
