Release Notes
---

## [0.10.0](https://github.com/substrait-io/substrait-java/compare/v0.9.0...v0.10.0) (2023-05-15)


### Features

* add comparison function mappings ([#142](https://github.com/substrait-io/substrait-java/issues/142)) ([77f2744](https://github.com/substrait-io/substrait-java/commit/77f27445828d70b3d0422518a835baf274da01f4))
* support proto <-> pojo custom type conversion ([#144](https://github.com/substrait-io/substrait-java/issues/144)) ([9a12e60](https://github.com/substrait-io/substrait-java/commit/9a12e60e33d1cb6b8e40a691951120d850ad86d6))
* upgrade calcite to 1.34.0 ([#146](https://github.com/substrait-io/substrait-java/issues/146)) ([02b0f17](https://github.com/substrait-io/substrait-java/commit/02b0f1781ab8e62e9fa2aace0e41813a8818a8bf))

## [0.9.0](https://github.com/substrait-io/substrait-java/compare/v0.8.0...v0.9.0) (2023-04-30)


### Features

* support for custom scalar and aggregate fns ([#140](https://github.com/substrait-io/substrait-java/issues/140)) ([8bd599a](https://github.com/substrait-io/substrait-java/commit/8bd599a3fac81bada9a15de54124cc611c1c891e))

## [0.8.0](https://github.com/substrait-io/substrait-java/compare/v0.7.0...v0.8.0) (2023-04-09)


### ⚠ BREAKING CHANGES

* Use commonExtension to indicate that these extensions
are associated with the RelCommon message
* Use relExtension to indicate that these extensions
are associated directly with Rels

### Features

* google.protobuf.Any handling ([#135](https://github.com/substrait-io/substrait-java/issues/135)) ([f7b7c3d](https://github.com/substrait-io/substrait-java/commit/f7b7c3d5286d4716f908d765fa43a893ad97a4fe))

## [0.7.0](https://github.com/substrait-io/substrait-java/compare/v0.6.0...v0.7.0) (2023-03-12)


### Features

* support output order remapping ([#132](https://github.com/substrait-io/substrait-java/issues/132)) ([6d94059](https://github.com/substrait-io/substrait-java/commit/6d94059329ad7f3b0cc2c124bed4f8b22b85551a))

## [0.6.0](https://github.com/substrait-io/substrait-java/compare/v0.5.0...v0.6.0) (2023-03-05)


### Features

* add FailureBehavior enum to Cast immutable ([#115](https://github.com/substrait-io/substrait-java/issues/115)) ([fd7cd5f](https://github.com/substrait-io/substrait-java/commit/fd7cd5f112c96a2bd4d41a3b4ea4522a7a372ce4))


### Bug Fixes

* incorrect mapping of floating point + and - ops ([#131](https://github.com/substrait-io/substrait-java/issues/131)) ([963c72f](https://github.com/substrait-io/substrait-java/commit/963c72f8bd403c6d8d2b6f6e095c788ff9627f13))

## [0.5.0](https://github.com/substrait-io/substrait-java/compare/v0.4.0...v0.5.0) (2023-02-05)


### Features

* upgrade substrait to 0.23.0 ([#124](https://github.com/substrait-io/substrait-java/issues/124)) ([63cfcb3](https://github.com/substrait-io/substrait-java/commit/63cfcb33fa7818e91303d79317c12b8489a9014f))


### Bug Fixes

* decimal parsing reversed ([#126](https://github.com/substrait-io/substrait-java/issues/126)) ([35aa306](https://github.com/substrait-io/substrait-java/commit/35aa306c4fda4ec5f24eb1fc83f295c49aea26d6))

## [0.4.0](https://github.com/substrait-io/substrait-java/compare/v0.3.0...v0.4.0) (2023-01-29)


### Features

* registered `upper` & `lower` in function mapping ([#103](https://github.com/substrait-io/substrait-java/issues/103)) ([c4f94db](https://github.com/substrait-io/substrait-java/commit/c4f94db24ba89ae653017c0f94bb0cc6a8fe9a96))

## [0.3.0](https://github.com/substrait-io/substrait-java/compare/v0.2.0...v0.3.0) (2023-01-19)


### ⚠ BREAKING CHANGES

* SubstraitRelNodeConverter constructor has changed

### Features

* Set RelBuilder directly in SubstraitRelNodeConverter ([#117](https://github.com/substrait-io/substrait-java/issues/117)) ([5c84515](https://github.com/substrait-io/substrait-java/commit/5c84515ac0cdde0e08a9d320c310bc26c82aad2e))

## [0.2.0](https://github.com/substrait-io/substrait-java/compare/v0.1.4...v0.2.0) (2022-12-04)


### Features

* support parsing of SQL queries with APPLY ([#106](https://github.com/substrait-io/substrait-java/issues/106)) ([4c81833](https://github.com/substrait-io/substrait-java/commit/4c81833688e71929c74a9a80e013174db88ad3ef))

## [0.1.4](https://github.com/substrait-io/substrait-java/compare/v0.1.3...v0.1.4) (2022-11-30)


### Bug Fixes

* open, close and promote from staging to maven ([#112](https://github.com/substrait-io/substrait-java/issues/112)) ([565e340](https://github.com/substrait-io/substrait-java/commit/565e340594f4576c3ab186f79c465378c3dc75ba))

## [0.1.3](https://github.com/substrait-io/substrait-java/compare/v0.1.2...v0.1.3) (2022-11-25)


### Bug Fixes

* quote signing key in sanity script ([#111](https://github.com/substrait-io/substrait-java/issues/111)) ([36ff257](https://github.com/substrait-io/substrait-java/commit/36ff2570497ed51360d286fee945641b8bc9b87d))

## [0.1.2](https://github.com/substrait-io/substrait-java/compare/v0.1.1...v0.1.2) (2022-11-25)


### Bug Fixes

* checkout submodules for release ([#110](https://github.com/substrait-io/substrait-java/issues/110)) ([919389e](https://github.com/substrait-io/substrait-java/commit/919389eee947aec3b7114fff340b6007c33952d9))

## [0.1.1](https://github.com/substrait-io/substrait-java/compare/v0.1.0...v0.1.1) (2022-11-25)


### Bug Fixes

* release build failing [#108](https://github.com/substrait-io/substrait-java/issues/108) ([#109](https://github.com/substrait-io/substrait-java/issues/109)) ([c0d7a7d](https://github.com/substrait-io/substrait-java/commit/c0d7a7d5889b483cf6e6e57d87379e5474011048))

## [0.1.0](https://github.com/substrait-io/substrait-java/compare/v0.0.0...v0.1.0) (2022-11-25)


### Features

* Add ReadRel LocalFiles ([#96](https://github.com/substrait-io/substrait-java/issues/96)) ([55d47d5](https://github.com/substrait-io/substrait-java/commit/55d47d5ba74139902c1d8d942a0ebece0526b3d8))
* publish to Sonatype OSSRH ([#102](https://github.com/substrait-io/substrait-java/issues/102)) ([7b195b0](https://github.com/substrait-io/substrait-java/commit/7b195b0e561a489ec0ead7cbd8de3a961659dbf6))
