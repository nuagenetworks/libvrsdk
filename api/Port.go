package api

import (
	"fmt"
	"github.com/nuagenetworks/libvrsdk/api/port"
	"github.com/nuagenetworks/libvrsdk/ovsdb"
	"github.com/socketplane/libovsdb"
	"reflect"
)

type empty struct{}

type ovsdbEventType string

const (
	add         ovsdbEventType = "ADD"
	update      ovsdbEventType = "UPDATE"
	porttable   string         = "Nuage_Port_Table"
)

// ClientEvent defines object type returned
// from OVSDB monitoring
type ClientEvent struct {
    EventData interface{}
}

// PortIPv4Info defines details to be populated
// for container port resolved in OVSDB
type PortIPv4Info struct {
	ipaddr  string
	gateway string
	mask    string
}

type ovsdbEvent struct {
	EventType   ovsdbEventType
	OvsdbObject interface{}
}

// GetAllPorts returns the slice of all the vport names attached to the VRS
func (vrsConnection *VRSConnection) GetAllPorts() ([]string, error) {

	readRowArgs := ovsdb.ReadRowArgs{
		Condition: []string{ovsdb.NuagePortTableColumnName, "!=", "xxxx"},
		Columns:   []string{ovsdb.NuagePortTableColumnName},
	}

	var nameRows []map[string]interface{}
	var err error
	if nameRows, err = vrsConnection.portTable.ReadRows(vrsConnection.ovsdbClient, readRowArgs); err != nil {
		return nil, fmt.Errorf("Unable to obtain the entity names %v", err)
	}

	var names []string
	for _, name := range nameRows {
		names = append(names, name[ovsdb.NuagePortTableColumnName].(string))
	}

	return names, nil
}

// CreatePort creates a new vPort in the Nuage VRS. The only mandatory inputs required to create
// a port are it's name and MAC address
func (vrsConnection *VRSConnection) CreatePort(name string, attributes port.Attributes,
	metadata map[port.MetadataKey]string) error {

	portMetadata := make(map[string]string)

	for k, v := range metadata {
		portMetadata[string(k)] = v
	}

	nuagePortRow := ovsdb.NuagePortTableRow{
		Name:             name,
		Mac:              attributes.MAC,
		Bridge:           attributes.Bridge,
		NuageDomain:      metadata[port.MetadataKeyDomain],
		NuageNetwork:     metadata[port.MetadataKeyNetwork],
		NuageNetworkType: metadata[port.MetadataKeyNetworkType],
		NuageZone:        metadata[port.MetadataKeyZone],
		VMDomain:         attributes.Platform,
		Metadata:         portMetadata,
	}

	if err := vrsConnection.portTable.InsertRow(vrsConnection.ovsdbClient, &nuagePortRow); err != nil {
		return fmt.Errorf("Problem adding port info to VRS %v", err)
	}

	return nil
}

// DestroyPort purges a port from the Nuage VRS
func (vrsConnection *VRSConnection) DestroyPort(name string) error {

	condition := []string{ovsdb.NuagePortTableColumnName, "==", name}
	if err := vrsConnection.portTable.DeleteRow(vrsConnection.ovsdbClient, condition); err != nil {
		return fmt.Errorf("Unable to remove the port from VRS %v", err)
	}

	return nil
}

// GetPortState gets the current resolution state of the port namely the IP address, Subnet Mask, Gateway,
// EVPN ID and VRF ID
func (vrsConnection VRSConnection) GetPortState(name string) (map[port.StateKey]interface{}, error) {

	readRowArgs := ovsdb.ReadRowArgs{
		Columns: []string{ovsdb.NuagePortTableColumnIPAddress, ovsdb.NuagePortTableColumnSubnetMask,
			ovsdb.NuagePortTableColumnGateway, ovsdb.NuagePortTableColumnEVPNID,
			ovsdb.NuagePortTableColumnVRFId},
		Condition: []string{ovsdb.NuagePortTableColumnName, "==", name},
	}

	var row map[string]interface{}
	var err error
	if row, err = vrsConnection.portTable.ReadRow(vrsConnection.ovsdbClient, readRowArgs); err != nil {
		return make(map[port.StateKey]interface{}), fmt.Errorf("Unable to obtain the port row %v", err)
	}

	portState := make(map[port.StateKey]interface{})
	portState[port.StateKeyIPAddress] = row[ovsdb.NuagePortTableColumnIPAddress]
	portState[port.StateKeySubnetMask] = row[ovsdb.NuagePortTableColumnSubnetMask]
	portState[port.StateKeyGateway] = row[ovsdb.NuagePortTableColumnGateway]
	portState[port.StateKeyVrfID] = row[ovsdb.NuagePortTableColumnVRFId]
	portState[port.StateKeyEvpnID] = row[ovsdb.NuagePortTableColumnEVPNID]

	return portState, nil
}

// UpdatePortAttributes updates the attributes of the vPort
func (vrsConnection *VRSConnection) UpdatePortAttributes(name string, attrs port.Attributes) error {
	row := make(map[string]interface{})

	row[ovsdb.NuagePortTableColumnBridge] = attrs.Bridge
	row[ovsdb.NuagePortTableColumnMAC] = attrs.MAC
	row[ovsdb.NuagePortTableColumnVMDomain] = attrs.Platform

	condition := []string{ovsdb.NuagePortTableColumnName, "==", name}

	if err := vrsConnection.portTable.UpdateRow(vrsConnection.ovsdbClient, row, condition); err != nil {
		return fmt.Errorf("Unable to update the port attributes %s %v %v", name, attrs, err)
	}

	return nil
}

// UpdatePortMetadata updates the metadata for the vPort
func (vrsConnection *VRSConnection) UpdatePortMetadata(name string, metadata map[string]string) error {
	row := make(map[string]interface{})

	metadataOVSDB, err := libovsdb.NewOvsMap(metadata)
	if err != nil {
		return fmt.Errorf("Unable to create OVSDB map %v", err)
	}

	row[ovsdb.NuagePortTableColumnMetadata] = metadataOVSDB

	key := string(port.MetadataKeyDomain)
	if len(metadata[key]) != 0 {
		row[ovsdb.NuagePortTableColumnNuageDomain] = metadata[key]
		delete(metadata, key)
	}

	key = string(port.MetadataKeyNetwork)
	if len(metadata[key]) != 0 {
		row[ovsdb.NuagePortTableColumnNuageNetwork] = metadata[key]
		delete(metadata, key)
	}

	key = string(port.MetadataKeyZone)
	if len(metadata[key]) != 0 {
		row[ovsdb.NuagePortTableColumnNuageZone] = metadata[key]
		delete(metadata, key)
	}

	condition := []string{ovsdb.NuagePortTableColumnName, "==", name}

	if err := vrsConnection.portTable.UpdateRow(vrsConnection.ovsdbClient, row, condition); err != nil {
		return fmt.Errorf("Unable to update the port metadata %s %v %v", name, metadata, err)
	}

	return nil
}

// GetNuagePortTableUpdate will register with OVSDB
// for Nuage Port table updates and return as soon as
// port table entry gets populated
func (vrsConnection *VRSConnection) GetNuagePortTableUpdate() <-chan ClientEvent {
	var err error
	clientChan := make(chan ClientEvent, 1)
        vrsConnection.updatesChan = make(chan *libovsdb.TableUpdates)
        vrsConnection.ovsdbClient.Register(vrsConnection)

	//set all monitors for ovsdb
	//set a monitor on Nuage_Port_Table
	tablesOfInterest := map[string]empty{"Nuage_Port_Table": {}}
	monitorRequests := make(map[string]libovsdb.MonitorRequest)
	schema, ok := vrsConnection.ovsdbClient.Schema["Open_vSwitch"]
	if !ok {
		return clientChan
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
	initialData, err := vrsConnection.ovsdbClient.Monitor("Open_vSwitch", nil, monitorRequests)
	if err != nil {
		return clientChan
	}

	go func() {
		clientEvent := &ClientEvent{}
                addTable, rowAdd := getEventOnTableUpdate(initialData, add)
                if addTable == true {
                        clientEvent = CreateObject(porttable, rowAdd, add)
                }
                if clientEvent != nil {
                	//clientChan <- *clientEvent
        	}
                for {
                        currentUpdate := <-vrsConnection.updatesChan
                        updateTable, rowUpdate := getEventOnTableUpdate(currentUpdate, update)
                        if updateTable == true {
                                clientEvent = CreateObject(porttable, rowUpdate, update)
                        }
                	if clientEvent != nil {
                        	clientChan <- *clientEvent
                	}
                }
        }()
        return clientChan
}

func getEventOnTableUpdate(data *libovsdb.TableUpdates, ovsdbEventType ovsdbEventType) (bool,libovsdb.Row) {

        for _, tableUpdate := range data.Updates {
                for _, row := range tableUpdate.Rows {
                        empty := libovsdb.Row{}
                        if !reflect.DeepEqual(row.New, empty) && ovsdbEventType == add {
                                return true, row.New
                        }

                        if !reflect.DeepEqual(row.New, empty) && !reflect.DeepEqual(row.Old, empty) && ovsdbEventType == update {
                                return true, row.New
                        }
                }
        }

        return false, libovsdb.Row{}
}

// CreateObject will create an object filled with
// required fields from OVSDB tables
func CreateObject(table string, row libovsdb.Row, ovsdbEventType ovsdbEventType) *ClientEvent {
	portIPv4Info := PortIPv4Info{}
	switch table {
	case porttable:

		if _, ok := row.Fields["ip_addr"]; ok {
			ip := row.Fields["ip_addr"].(string)
			portIPv4Info.ipaddr = ip
		}
		if _, ok := row.Fields["gateway"]; ok {
			gw := row.Fields["gateway"].(string)
			portIPv4Info.gateway = gw
		}
		if _, ok := row.Fields["subnet_mask"]; ok {
			subnetmask := row.Fields["subnet_mask"].(string)
			portIPv4Info.mask = subnetmask
		}
        clientEvent := ClientEvent{ovsdbEvent{
            EventType:   ovsdbEventType,
            OvsdbObject: portIPv4Info}, //fixme
        }
        return &clientEvent
    	default:
      		return nil
    }
}
