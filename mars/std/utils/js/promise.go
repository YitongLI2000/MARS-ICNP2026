//go:build js && wasm

package js

import (
	"errors"
	"syscall/js"
)

var promiseGlobal = js.Global().Get("Promise")

func AsyncFunc(f func(this js.Value, p []js.Value) (any, error)) js.Func {
	return js.FuncOf(func(this js.Value, p []js.Value) any {
		promise, resolve, reject := Promise()
		go func() {
			ret, err := f(this, p)
			if err != nil {
				reject(err.Error())
			} else {
				resolve(ret)
			}
		}()
		return promise
	})
}

func Promise() (promise js.Value, resolve func(args ...any), reject func(args ...any)) {
	var jsResolve, jsReject js.Value

	promiseConstructor := js.FuncOf(func(this js.Value, args []js.Value) any {
		jsResolve = args[0]
		jsReject = args[1]
		return nil
	})

	promise = promiseGlobal.New(promiseConstructor)
	resolve = func(args ...any) { jsResolve.Invoke(args...) }
	reject = func(args ...any) { jsReject.Invoke(args...) }

	promiseConstructor.Release()
	return
}

func Await(promise js.Value) (val js.Value, err error) {
	res := make(chan any, 1)

	var resolve, reject js.Func
	resolve = js.FuncOf(func(this js.Value, p []js.Value) any {
		res <- p[0]
		resolve.Release()
		reject.Release()
		return nil
	})
	reject = js.FuncOf(func(this js.Value, p []js.Value) any {
		res <- errors.New(p[0].String())
		resolve.Release()
		reject.Release()
		return nil
	})

	promise.Call("then", resolve).Call("catch", reject)

	result := <-res
	switch v := result.(type) {
	case js.Value:
		return v, nil
	case error:
		return js.Undefined(), v
	default:
		return js.Undefined(), errors.New("unexpected type")
	}
}
