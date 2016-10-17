package api

import (
	"github.com/nuagenetworks/libvrsdk/ovsdb"
	"github.com/socketplane/libovsdb"
)

// VRSConnection represent the OVSDB connection to the VRS
type VRSConnection struct {
	ovsdbClient *libovsdb.OvsdbClient
	vmTable     ovsdb.NuageTable
	portTable   ovsdb.NuageTable
	updatesChan chan *libovsdb.TableUpdates
}

// Disconnected will retry connecting to OVSDB
// and continue to register for OVSDB updates
func (vrsConnection VRSConnection) Disconnected(ovsClient *libovsdb.OvsdbClient) {
}

// Locked is a placeholder function
func (vrsConnection VRSConnection) Locked([]interface{}) {
}

// Stolen is a placeholder function
func (vrsConnection VRSConnection) Stolen([]interface{}) {
}

// Echo is a placeholder function
func (vrsConnection VRSConnection) Echo([]interface{}) {
}

// Update will provide updates on OVSDB table updates
func (vrsConnection VRSConnection) Update(context interface{}, tableUpdates libovsdb.TableUpdates) {
        vrsConnection.updatesChan <- &tableUpdates
}

// NewUnixSocketConnection creates a connection to the VRS Server using Unix sockets
func NewUnixSocketConnection(socketfile string) (VRSConnection, error) {
	var vrsConnection VRSConnection
	var err error

	if vrsConnection.ovsdbClient, err = libovsdb.ConnectWithUnixSocket(socketfile); err != nil {
		return vrsConnection, err
	}

	vrsConnection.vmTable.TableName = ovsdb.NuageVMTable
	vrsConnection.portTable.TableName = ovsdb.NuagePortTable

	return vrsConnection, nil
}

// Disconnect closes the connection to the VRS server
func (vrsConnection VRSConnection) Disconnect() {
	vrsConnection.ovsdbClient.Disconnect()
}
