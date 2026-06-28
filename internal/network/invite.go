// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mr-tron/base58"
	ma "github.com/multiformats/go-multiaddr"
)

// Invite encoding, per spec:
//
//	psnode://v1/<base58(version_byte + addrs + psk_32 + label_utf8)>
//
// Binary body layout:
//
//	[version:1]
//	[numAddrs:1]
//	numAddrs × ( [addrLen:uint16 BE] [addr bytes] )   // multiaddr binary form
//	[psk:32]
//	[label: remaining bytes, UTF-8]
const (
	inviteScheme  = "psnode://v1/"
	inviteVersion = 0x01
	pskLen        = 32
)

// Invite is a decoded private-network invite.
type Invite struct {
	Multiaddrs []ma.Multiaddr
	PSK        []byte
	Label      string
}

// EncodeInvite serializes an invite to its psnode:// string form.
func EncodeInvite(inv Invite) (string, error) {
	if len(inv.PSK) != pskLen {
		return "", fmt.Errorf("psk must be %d bytes, got %d", pskLen, len(inv.PSK))
	}
	if len(inv.Multiaddrs) == 0 {
		return "", errors.New("invite requires at least one multiaddr")
	}
	if len(inv.Multiaddrs) > 255 {
		return "", errors.New("invite supports at most 255 multiaddrs")
	}

	var buf bytes.Buffer
	buf.WriteByte(inviteVersion)
	buf.WriteByte(byte(len(inv.Multiaddrs)))
	for _, a := range inv.Multiaddrs {
		ab := a.Bytes()
		if len(ab) > 0xFFFF {
			return "", errors.New("multiaddr too long to encode")
		}
		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(ab)))
		buf.Write(lenBuf[:])
		buf.Write(ab)
	}
	buf.Write(inv.PSK)
	buf.WriteString(inv.Label)

	return inviteScheme + base58.Encode(buf.Bytes()), nil
}

// DecodeInvite parses a psnode:// invite string.
func DecodeInvite(s string) (Invite, error) {
	var inv Invite
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, inviteScheme) {
		return inv, fmt.Errorf("invite must start with %q", inviteScheme)
	}
	raw, err := base58.Decode(strings.TrimPrefix(s, inviteScheme))
	if err != nil {
		return inv, fmt.Errorf("invite base58 decode: %w", err)
	}

	r := bytes.NewReader(raw)
	version, err := r.ReadByte()
	if err != nil {
		return inv, errors.New("invite truncated (version)")
	}
	if version != inviteVersion {
		return inv, fmt.Errorf("unsupported invite version 0x%02x", version)
	}
	numAddrs, err := r.ReadByte()
	if err != nil {
		return inv, errors.New("invite truncated (addr count)")
	}

	for i := 0; i < int(numAddrs); i++ {
		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return inv, errors.New("invite truncated (addr length)")
		}
		n := binary.BigEndian.Uint16(lenBuf[:])
		ab := make([]byte, n)
		if _, err := io.ReadFull(r, ab); err != nil {
			return inv, errors.New("invite truncated (addr bytes)")
		}
		a, err := ma.NewMultiaddrBytes(ab)
		if err != nil {
			return inv, fmt.Errorf("invite multiaddr %d invalid: %w", i, err)
		}
		inv.Multiaddrs = append(inv.Multiaddrs, a)
	}

	psk := make([]byte, pskLen)
	if _, err := io.ReadFull(r, psk); err != nil {
		return inv, errors.New("invite truncated (psk)")
	}
	inv.PSK = psk

	label := make([]byte, r.Len())
	if _, err := io.ReadFull(r, label); err != nil {
		return inv, errors.New("invite truncated (label)")
	}
	inv.Label = string(label)

	return inv, nil
}
