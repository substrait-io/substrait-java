# Install & build

There are two ways to get the `isthmus` binary: download a prebuilt native binary from a
GitHub release, or build it yourself from source with GraalVM.

## Download a prebuilt binary

Every [substrait-java release](https://github.com/substrait-io/substrait-java/releases)
attaches prebuilt native-image binaries for Linux and macOS. The assets are named after
the release version:

- `isthmus-ubuntu-<version>` — Linux (x86-64)
- `isthmus-macOS-<version>` — macOS

Download the asset that matches your platform, then make it executable and (optionally)
put it on your `PATH`:

```bash
# Example: after downloading isthmus-ubuntu-<version> for Linux
chmod +x isthmus-ubuntu-<version>
mv isthmus-ubuntu-<version> /usr/local/bin/isthmus

isthmus --version
```

The binaries are standalone native images: no JVM or Java installation is required to run
them.

## Build from source

Isthmus is built as a native executable via GraalVM. From the repository root, run:

```bash
./gradlew nativeCompile
```

The resulting binary is written to:

```text
isthmus-cli/build/native/nativeCompile/isthmus
```

!!! warning "GraalVM 25 with `native-image` is required"
    `nativeCompile` runs the GraalVM `native-image` tool with whatever JDK is running the
    Gradle daemon. You must have a **GraalVM 25 JDK** (with the `native-image` component)
    installed and its location set in the `GRAALVM_HOME` environment variable. This is a
    different toolchain from the JDK 17 used to build the rest of the project.

Verify the build:

```bash
./isthmus-cli/build/native/nativeCompile/isthmus --version
```

which prints the binary and Substrait spec versions:

```text
isthmus version <version>
Substrait version <substrait-version>
```

!!! tip "Referencing the binary in scripts"
    The bundled smoke-test scripts resolve the binary from the `ISTHMUS` environment
    variable, falling back to `build/native/nativeCompile/isthmus` (relative to the
    `isthmus-cli` module). Set `ISTHMUS` to point at a downloaded binary if you want to run
    the scripts against a release build. See [Examples](examples.md).
