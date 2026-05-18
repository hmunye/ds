package messenger

import (
	"testing"
)

func TestParseMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "single message",
			input: `{
                "src": "c1",
                "dest": "n1",
                "body": {
                    "type": "echo",
                    "msg_id": 1
                }
            }`,
			expected: "PARSED: c1|n1|echo",
		},
		{
			name: "missing type field",
			input: `{
                "src": "c1",
                "dest": "n1",
                "body": {
                    "msg_id": 1
                }
            }`,
			expected: "PARSED: c1|n1|unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := ParseMessage([]byte(tt.input))
			if err != nil {
				t.Fatalf("error: %v", err)
			}

			parsed := msg.FormatMessage()

			if parsed != tt.expected {
				t.Errorf("got %v, want %v", parsed, tt.expected)
			}
		})
	}
}
