//go:build js && wasm

package js

import "syscall/js"

var arrayBuffer = js.Global().Get("ArrayBuffer")
var uint8Array = js.Global().Get("Uint8Array")

func SliceToJsArray(slice []byte) js.Value {
	jsSlice := uint8Array.New(len(slice))
	js.CopyBytesToJS(jsSlice, slice)
	return jsSlice
}

func JsArrayToSlice(jsArray js.Value) []byte {
	if jsArray.InstanceOf(arrayBuffer) {
		jsArray = uint8Array.New(jsArray)
	}

	slice := make([]byte, jsArray.Get("byteLength").Int())
	js.CopyBytesToGo(slice, jsArray)
	return slice
}
