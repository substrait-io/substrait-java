Release Notes
---

## [0.18.0](https://github.com/substrait-io/substrait-java/compare/v0.17.0...v0.18.0) (2023-09-24)


### ⚠ BREAKING CHANGES

* StrLiteral is no longer converted to CHAR(<length>)

fix(isthmus): convert BinaryLiteral to VARBINARY
* BinaryLiteral is no longer converted to BINARY<length>)
* **calcite:** Isthmus no longer uses Calcite built-in MAX, MIN, SUM, SUM0 and AVG functions
* **calcite:** removed REQUIRED and NULLABLE fields from Type interface

### Features

* **calcite:** dedicated Substrait MAX, MIN, SUM, SUM0 and AVG functions ([#180](https://github.com/substrait-io/substrait-java/issues/180)) ([477b63e](https://github.com/substrait-io/substrait-java/commit/477b63e0b2b3bb2caf877a5cc9518fd08b2f6ea0))
* extend literal conversion support ([#183](https://github.com/substrait-io/substrait-java/issues/183)) ([6e82f39](https://github.com/substrait-io/substrait-java/commit/6e82f397a168f3811593fe8248753d70d4f0c04f))


### Bug Fixes

* support any<n>? type syntax in function extensions ([#184](https://github.com/substrait-io/substrait-java/issues/184)) ([16e5604](https://github.com/substrait-io/substrait-java/commit/16e56046cec47a7913975817d780224c95a926e4))

## [0.17.0](https://github.com/substrait-io/substrait-java/compare/v0.16.0...v0.17.0) (2023-09-17)


### ⚠ BREAKING CHANGES

* EnumArgument, TypeArgument and ValueArgument are now
abstract
* visit over core substrait types (#178)

### Features

* **calcite:** support reading in list and map literals ([#177](https://github.com/substrait-io/substrait-java/issues/177)) ([e8a2645](https://github.com/substrait-io/substrait-java/commit/e8a264539d35f2acee8b720e8292796f4d4825d1))
* use Immutables for Argument subclasses ([#179](https://github.com/substrait-io/substrait-java/issues/179)) ([edfc65f](https://github.com/substrait-io/substrait-java/commit/edfc65f1606f38e3dffd5fc9a294bf7365455186))
* visit over core substrait types ([#178](https://github.com/substrait-io/substrait-java/issues/178)) ([6bee452](https://github.com/substrait-io/substrait-java/commit/6bee452bb9070c42d1f06ac2c3387c5440a4ba1b))


### Bug Fixes

* **calcite:** use upperBound util when converting upperBound ([#176](https://github.com/substrait-io/substrait-java/issues/176)) ([b90432d](https://github.com/substrait-io/substrait-java/commit/b90432d8cc06d8c674abeed848cc4f2fe57d734d))

## [0.16.0](https://github.com/substrait-io/substrait-java/compare/v0.15.0...v0.16.0) (2023-09-06)


### ⚠ BREAKING CHANGES

* * windowFunction expression creator now requires window bound type parameter
* the WindowBound POJO representation has been reworked to use visitation and more closely match the spec
* ExpressionRexConverter now requires a WindowFunctionConverter

* * feat: convert Substrait window functions to Calcite RexOvers (#172) ([7618bb8](https://github.com/substrait-io/substrait-java/commit/7618bb82150a430c3a8a9621c28e983e66785230)), closes [#172](https://github.com/substrait-io/substrait-java/issues/172)

## [0.15.0](https://github.com/substrait-io/substrait-java/compare/v0.14.1...v0.15.0) (2023-08-20)


### ⚠ BREAKING CHANGES

* **pojos:** various public functions that took the
AggregateFunction.AggregationInvocation proto now take the POJO
equivalent Expression.AggregationInvocation.

### Features

* implement fractional second intervals ([#167](https://github.com/substrait-io/substrait-java/issues/167)) ([68aa7c4](https://github.com/substrait-io/substrait-java/commit/68aa7c4782dbede50407d6b9c2cfa739b07b73c6))
* support reading Substrait plans with Window Functions  ([#165](https://github.com/substrait-io/substrait-java/issues/165)) ([93c6db5](https://github.com/substrait-io/substrait-java/commit/93c6db525d15a50e509ec539a3a9dbfc05d3cb91))


### Code Refactoring

* **pojos:** avoid using raw proto enums in POJO layer ([#164](https://github.com/substrait-io/substrait-java/issues/164)) ([41c8400](https://github.com/substrait-io/substrait-java/commit/41c840028c300cfdbc28ca21e68529058dbc8f05))

## [0.14.1](https://github.com/substrait-io/substrait-java/compare/v0.14.0...v0.14.1) (2023-08-06)


### Bug Fixes

* handle custom extensions through expressions ([#161](https://github.com/substrait-io/substrait-java/issues/161)) ([af91dc3](https://github.com/substrait-io/substrait-java/commit/af91dc3590ed07a5f9b91a75faf07ce30a0f7218))

## [0.14.0](https://github.com/substrait-io/substrait-java/compare/v0.13.0...v0.14.0) (2023-07-23)


### Features

* Add SingleOrList support to the Isthmus converter ([#159](https://github.com/substrait-io/substrait-java/issues/159)) ([297c535](https://github.com/substrait-io/substrait-java/commit/297c535875b5084967c27d7f72639c16b9563477))

## [0.13.0](https://github.com/substrait-io/substrait-java/compare/v0.12.1...v0.13.0) (2023-07-16)


### Features

* add missing getters to ValueArgument ([#158](https://github.com/substrait-io/substrait-java/issues/158)) ([46f0b9f](https://github.com/substrait-io/substrait-java/commit/46f0b9f3007137a00f876c85fbc328e419fd1576))


### Bug Fixes

* left/right/outer joins have nullable fields ([#157](https://github.com/substrait-io/substrait-java/issues/157)) ([b987058](https://github.com/substrait-io/substrait-java/commit/b9870581ec53a54880238cd8d2cf5b377599da55))

## [0.12.1](https://github.com/substrait-io/substrait-java/compare/v0.12.0...v0.12.1) (2023-06-18)


### Bug Fixes

* extension merging should include types ([#152](https://github.com/substrait-io/substrait-java/issues/152)) ([5718537](https://github.com/substrait-io/substrait-java/commit/57185375d960dc979e94581980e76eaa0d0e945c))

## [0.12.0](https://github.com/substrait-io/substrait-java/compare/v0.11.0...v0.12.0) (2023-06-06)


### ⚠ BREAKING CHANGES

* TypeConverter no longer uses static methods
* SimpleExtension.MAPPER has been replaced with SimpleExtension.objectMapper(String namespace)

### Features

* handle user-defined types in Isthmus ([#149](https://github.com/substrait-io/substrait-java/issues/149)) ([7d7acf8](https://github.com/substrait-io/substrait-java/commit/7d7acf8b46a8dfd6cf64079cb9d82e3bb782f4a1))
* support IfThen translation from Substrait to Calcite ([#151](https://github.com/substrait-io/substrait-java/issues/151)) ([f505c23](https://github.com/substrait-io/substrait-java/commit/f505c23b905b45bfd069296cae3cf67a2f77e9cc))

## [0.11.0](https://github.com/substrait-io/substrait-java/compare/v0.10.0...v0.11.0) (2023-05-28)


### ⚠ BREAKING CHANGES

* rename and move extension associated code (#148)

### Code Refactoring

* rename and move extension associated code ([#148](https://github.com/substrait-io/substrait-java/issues/148)) ([6f29d32](https://github.com/substrait-io/substrait-java/commit/6f29d32be0408bc3da7fe9a7a85ab3d269d1f670))

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
