# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for fields containing dots in their name as literals ([#19958](https://github.com/opensearch-project/OpenSearch/pull/19958))
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))
- Added public getter method in `SourceFieldMapper` to return excluded field ([#20205](https://github.com/opensearch-project/OpenSearch/pull/20205))
- Add integ test for simulating node join left event when data node cluster state publication lag because the cluster applier thread being busy ([#19907](https://github.com/opensearch-project/OpenSearch/pull/19907)).
- Relax jar hell check when extended plugins share transitive dependencies ([#20103](https://github.com/opensearch-project/OpenSearch/pull/20103))
- Added public getter method in `SourceFieldMapper` to return included field ([#20290](https://github.com/opensearch-project/OpenSearch/pull/20290))
- Support for HTTP/3 (server side) ([#20017](https://github.com/opensearch-project/OpenSearch/pull/20017))
- Add circuit breaker support for gRPC transport to prevent out-of-memory errors ([#20203](https://github.com/opensearch-project/OpenSearch/pull/20203))
- Add index-level-encryption support for snapshots and remote-store ([#20095](https://github.com/opensearch-project/OpenSearch/pull/20095))
- Adding BackWardCompatibility test for remote publication enabled cluster ([#20221](https://github.com/opensearch-project/OpenSearch/pull/20221))
- Support for hll field mapper to support cardinality rollups ([#20129](https://github.com/opensearch-project/OpenSearch/pull/20129))
- Add tracing support for StreamingRestChannel  ([#20361](https://github.com/opensearch-project/OpenSearch/pull/20361))
- Introduce new libs/netty4 module to share common implementation between netty-based plugins and modules (transport-netty4, transport-reactor-netty4) ([#20447](https://github.com/opensearch-project/OpenSearch/pull/20447))
- Add validation to make crypto store settings immutable ([#20123](https://github.com/opensearch-project/OpenSearch/pull/20123))
- Introduce concurrent translog recovery to accelerate segment replication primary promotion ([#20251](https://github.com/opensearch-project/OpenSearch/pull/20251))
- Update to `almalinux:10` ([#20482](https://github.com/opensearch-project/OpenSearch/pull/20482))
- Add X-Request-Id to uniquely identify a search request ([#19798](https://github.com/opensearch-project/OpenSearch/pull/19798))
- Added TopN selection logic for streaming terms aggregations ([#20481](https://github.com/opensearch-project/OpenSearch/pull/20481))
- Added support for Intra Segment Search ([#19704](https://github.com/opensearch-project/OpenSearch/pull/19704))
- Introduce AdditionalCodecs and EnginePlugin::getAdditionalCodecs hook to allow additional Codec registration ([#20411](https://github.com/opensearch-project/OpenSearch/pull/20411))
- Add warmup phase to wait for lag to catch up in pull-based ingestion before serving ([#20526](https://github.com/opensearch-project/OpenSearch/pull/20526))

### Changed

### Fixed
- Prevent criteria update for context aware indices ([#20250](https://github.com/opensearch-project/OpenSearch/pull/20250))

### Dependencies
- Bump `ch.qos.logback:logback-core` and `ch.qos.logback:logback-classic` from 1.5.24 to 1.5.27 ([#20525](https://github.com/opensearch-project/OpenSearch/pull/20525))

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.5...main
