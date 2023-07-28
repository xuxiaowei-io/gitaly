package config

import (
	"encoding/json"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/errors/cfgerror"
)

// Node describes an address that serves a storage
type Node struct {
	Storage string `toml:"storage,omitempty" json:"storage"`
	Address string `toml:"address,omitempty" json:"address"`
	Token   string `toml:"token,omitempty" json:"token"`
}

//nolint:revive // This is unintentionally missing documentation.
func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"storage": n.Storage,
		"address": n.Address,
	})
}

// String prints out the node attributes but hiding the token
func (n Node) String() string {
	return fmt.Sprintf("storage_name: %s, address: %s", n.Storage, n.Address)
}

// Validate runs validation on all fields and compose all found errors.
func (n Node) Validate() error {
	return cfgerror.New().
		Append(cfgerror.NotBlank(n.Storage), "storage").
		Append(cfgerror.NotBlank(n.Address), "address").
		AsError()
}
