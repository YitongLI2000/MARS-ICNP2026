all: main.wasm

main.wasm: clean main.go
	cp $(go env GOROOT)/usr/local/go/misc/wasm/wasm_exec.js .
	GOOS=js GOARCH=wasm go build -o main.wasm

clean:
	rm -f main.wasm wasm_exec.js

serve:
	python3 -m http.server
