# package-common

Shared maven-assembly-plugin descriptor for the per-flavor dimster package
modules. Not a Maven module itself - just a directory holding `bin.xml`, which
is referenced from each `package-<flavor>/pom.xml` via a relative
`<descriptor>../package-common/src/assemble/bin.xml</descriptor>` path.

The descriptor produces the binary tarball (Java jars in `lib/`, scripts in
`bin/`, workloads in `workloads/`, etc.). It deliberately does NOT use
`useAllReactorProjects` because that would pull every per-flavor impl module's
jar into every tarball, which would put multiple `KafkaClientFactory`
implementations on the runtime classpath - `KafkaSpi.factory()` then errors
out with "Multiple implementations found". Each `package-<flavor>` module
controls what lands in `lib/` purely through its own dependency declarations.
