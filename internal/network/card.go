// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// DiscoveryTopic is the global, cross-network GossipSub topic on which network
// cards are gossiped. It is NOT network-scoped: nodes participate regardless of
// their own network (except private networks, which never join it).
const DiscoveryTopic = "plural-star:discovery:networks"

// NetworkCard advertises a public network. It is self-signed by its creator;
// the signature covers every field except Signature itself.
type NetworkCard struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	BootstrapPeers []string `json:"bootstrap_peers"`
	NodeCountHint  int      `json:"node_count_hint"`
	CreatedBy      string   `json:"created_by"` // creator peer ID
	CreatedAt      int64    `json:"created_at"`
	Signature      string   `json:"signature"` // base64 Ed25519 signature
}

// signingBytes returns the canonical bytes signed/verified: the card with an
// empty Signature field. Struct marshaling preserves field order, so this is
// deterministic.
func (c NetworkCard) signingBytes() ([]byte, error) {
	c.Signature = ""
	return json.Marshal(c)
}

// SignNetworkCard sets CreatedBy to priv's peer ID and fills Signature.
func SignNetworkCard(c *NetworkCard, priv crypto.PrivKey) error {
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("derive peer id: %w", err)
	}
	c.CreatedBy = pid.String()
	b, err := c.signingBytes()
	if err != nil {
		return err
	}
	sig, err := priv.Sign(b)
	if err != nil {
		return fmt.Errorf("sign card: %w", err)
	}
	c.Signature = base64.StdEncoding.EncodeToString(sig)
	return nil
}

// VerifyNetworkCard checks the self-signature against the CreatedBy peer ID.
func VerifyNetworkCard(c *NetworkCard) error {
	if c.Signature == "" {
		return errors.New("card has no signature")
	}
	pid, err := peer.Decode(c.CreatedBy)
	if err != nil {
		return fmt.Errorf("invalid created_by: %w", err)
	}
	pub, err := pid.ExtractPublicKey()
	if err != nil || pub == nil {
		return fmt.Errorf("cannot extract public key from created_by peer id: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	b, err := c.signingBytes()
	if err != nil {
		return err
	}
	ok, err := pub.Verify(b, sig)
	if err != nil {
		return fmt.Errorf("verify signature: %w", err)
	}
	if !ok {
		return errors.New("card signature does not match created_by key")
	}
	return nil
}
