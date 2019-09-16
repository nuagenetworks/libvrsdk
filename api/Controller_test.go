package api

import "testing"

func TestGetControllerState(t *testing.T) {
	var err error
	var state ControllerState
	var vrsConnection VRSConnection

	vrsConnection, err = NewUnixSocketConnection(UnixSocketFile)
	if err != nil {
		t.Fatal("Unable to connect to the VRS")
	}

	state, err = vrsConnection.GetControllerState()
	if err != nil {
		t.Fatalf("error getting controller state")
	}

	if state != ControllerConnected {
		t.Fatalf("controller state is not connected")
	}
}
