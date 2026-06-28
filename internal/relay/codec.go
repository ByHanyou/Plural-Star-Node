// SPDX-License-Identifier: AGPL-3.0-or-later

package relay

import "github.com/vmihailenco/msgpack/v5"

// Marshal serializes the packet with msgpack. Both ends are this same node
// binary, so msgpack (compact, no codegen) is sufficient for the wire format.
func (p *Packet) Marshal() ([]byte, error) {
	return msgpack.Marshal(p)
}

// UnmarshalPacket parses a msgpack-encoded packet.
func UnmarshalPacket(b []byte) (*Packet, error) {
	var p Packet
	if err := msgpack.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
