//go:build js && wasm

package js

import "syscall/js"

func ReleaseMap(m map[string]any) {
	for _, val := range m {
		if val, ok := val.(js.Func); ok {
			val.Release()
		}
	}
}
