## Bindings for schema parsing

These bindings require [maturin](https://github.com/PyO3/maturin) and Rust
installed.

We've tested with Rust 1.74.

## Project structure

The Rust bindings are in `src/lib.rs`. They are simple exports of functionality
from `concordium-contracts-common`.

The entrypoint for Python consumers is the `schema_parsing` package which has a
single class `Schema`. The constructor will instantiate the schema from a
deployed Wasm module.

After that the constructed object can be used to parse events or return values
using the schema in the module.


```python
f = open('/path/to/module', 'rb').read()
schema = Schema(f)
# to convert the event which is serialized as [1,2,3] for the contract "test"
# to json
event_json = s.event_to_json("test", [1,2,2])
```


## Building

Run `maturin build`.

This will produce a python wheel in `target/wheels` that will contain both the
compiled Rust binaries and python wrappers. The compiled package is platform
specific, so a package built on, e.g., Linux will not work on Windows.

