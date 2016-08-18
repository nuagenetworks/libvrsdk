package api

import (
	"fmt"
	"github.com/nuagenetworks/libvrsdk/api/port"
	"github.com/nuagenetworks/libvrsdk/ovsdb"
	"github.com/socketplane/libovsdb"
)

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
