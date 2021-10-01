package network

import "testing"

func TestNewSimpleNetwork(t *testing.T) {
	sn := NewSimpleNetwork()
	if len(sn.heap) != 0 || len(sn.connections) != 0 {
		t.Fatalf("NewSimpleNetwork does not return empty network")
	}
}

func TestSNAddConnection(t *testing.T) {
	t.Fatalf("test not implemented")
	sn := NewSimpleNetwork()
}

func TestSNStartTransfer(t *testing.T) {
	t.Fatalf("test not implemented")
}

func TestSNAdvance(t *testing.T) {
	t.Fatalf("test not implemented")
}

func TestSNStatus(t *testing.T) {
	t.Fatalf("test not implemented")
}
