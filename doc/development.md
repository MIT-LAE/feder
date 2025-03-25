# Feder development

## Package layout


## Building a distributable API package

The API code is in the `api` directory with a project name of "`feder`" to
make life as easy for users as possible. To build a source and wheel
distribution for the API, do the following at the top-level of the repo:

```shell
uv build --package feder
```

The results are put into the `dist` directory.

Because the API needs access to some common code shared with the server-side
code and because it's very hard to get Python build tools to bundle local
dependencies into build artefacts, a small trick is used to allow common code
to be imported within the API as packaged under `feder.common`: the source
directory of the `feder-common` package is symlinked alongside the source
directory for the `feder.api` package. This means that API code can access
`feder.common` imports and distribution artefacts include the shared code in a
reasonable way. This isn't the ideal solution to this problem, and it requires
a little care to make sure that the common code works both as a local
dependency used in the server code and as this "vendored" dependency in the
API package.


## Protocol Buffers

Feder uses [Protocol Buffers](https://protobuf.dev/) to encode trajectory data
in binary fields in the SQLite3 database files. The schema for these files is
in `schemas/points.proto` and needs to be compiled into a Python module in the
`feder-common` library. There is a Makefile in the `schemas` directory to do
this, but you need a working version of the Protocol Buffers compiler
installed for it to work! For setting this up on Hex, the instructions
[here](https://protobuf.dev/installation/) work fine.



