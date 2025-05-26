package usermgt

import (
	"github.com/nspcc-dev/neo-go/pkg/interop"
	"github.com/nspcc-dev/neo-go/pkg/interop/convert"
	"github.com/nspcc-dev/neo-go/pkg/interop/native/crypto"
	"github.com/nspcc-dev/neo-go/pkg/interop/runtime"
)

var userPub = []byte{3, 82, 213, 129, 152, 145, 189, 105, 89, 174, 158, 60, 235, 97, 172, 163, 108, 102, 177, 14, 80, 147, 80, 130, 37, 187, 89, 163, 108, 182, 213, 170, 53}

func VerifySignature(user string, sig interop.Signature) bool {
	if user != "Bob" {
		panic("wrong user " + user)
	}

	ntb := make([]byte, 4)
	copy(ntb, convert.ToBytes(runtime.GetNetwork()))

	signedPayload := append(ntb, runtime.GetScriptContainer().Hash...)

	return crypto.VerifyWithECDsa(signedPayload, userPub, sig, crypto.Secp256r1Sha256)
}
