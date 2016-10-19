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
	lastUpdateData *libovsdb.TableUpdates
}

// Disconnected will retry connecting to OVSDB
// and continue to register for OVSDB updates
func (vrsConnection VRSConnection) Disconnected(ovsClient *libovsdb.OvsdbClient) {
}

// Locked is a placeholder function for table updates
func (vrsConnection VRSConnection) Locked([]interface{}) {
}

// Stolen is a placeholder function for table updates
func (vrsConnection VRSConnection) Stolen([]interface{}) {
}

// Echo is a placeholder function for table updates
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

        // Setting a monitor on Nuage_Port_Table in VRS connection
        vrsConnection.updatesChan = make(chan *libovsdb.TableUpdates)
        vrsConnection.ovsdbClient.Register(vrsConnection)
        tablesOfInterest := map[string]empty{"Nuage_Port_Table": {}}
        monitorRequests := make(map[string]libovsdb.MonitorRequest)
        schema, ok := vrsConnection.ovsdbClient.Schema["Open_vSwitch"]
        if !ok {
                return vrsConnection, err
        }

        for table, tableSchema := range schema.Tables {
                if _, interesting := tablesOfInterest[table]; interesting {
                        var columns []string
                        for column := range tableSchema.Columns {
                                columns = append(columns, column)
                        }
                        monitorRequests[table] = libovsdb.MonitorRequest{
                                Columns: columns,
                                Select: libovsdb.MonitorSelect{
                                        Initial: true,
                                        Modify:  true}}
                }
        }
        vrsConnection.lastUpdateData, err = vrsConnection.ovsdbClient.Monitor("Open_vSwitch", nil, monitorRequests)
        if err != nil {
                return vrsConnection, err
        }
        return vrsConnection, nil
}

// Disconnect closes the connection to the VRS server
func (vrsConnection VRSConnection) Disconnect() {
	vrsConnection.ovsdbClient.Disconnect()
}
