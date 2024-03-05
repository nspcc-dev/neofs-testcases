package testdata

import (
	"github.com/nspcc-dev/neo-go/pkg/interop"
	"github.com/nspcc-dev/neo-go/pkg/interop/contract"
	"github.com/nspcc-dev/neo-go/pkg/interop/native/management"
	"github.com/nspcc-dev/neo-go/pkg/interop/runtime"
)

func _deploy(data interface{}, isUpdate bool) {
	if !isUpdate && data.(string) == "shouldFail" {
		panic("deploy has failed")
	}
}

// GetThree is a simple function which does nothing.
func GetThree() int {
	return 3
}

// Update allows to update this contract.
func Update(nef, manifest []byte, data interface{}) {
	contract.Call(interop.Hash160(management.Hash), "update",
		contract.All, nef, manifest, data)
	runtime.Log("test contract was updated")
}
