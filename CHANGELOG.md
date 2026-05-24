# Changelog

All notable user-visible changes to Dimster are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/) under the 0.x
relaxed rules described in [RELEASING.md](RELEASING.md).

While Dimster is on 0.x, **minor version bumps may include breaking changes**
— to the workload YAML schema, broker-config schema, CLI flags, K8s manifest
layout, or results JSON schema. Patch bumps are bug fixes and additive
non-breaking changes only.

## [Unreleased]

### Added

### Changed

## [0.1.0]

Initial public release.

## [0.1.1]

### Added

- Users can set environment variables in Kafka pods via the Dimster config.

### Changed

- Restarting producers and consumers as part of a --modify command now restarts them in parallel, rather than sequentially.

## [0.2.0]

### Changed

- Live-interaction: Changes to workload fields now done via the --set sub-command and workload fields use field=value format (breaking change).
- Live-interaction: Rejects unrecognized sub-commands and unsupported workload fields.