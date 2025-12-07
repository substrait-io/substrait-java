Release Notes
---

## [0.72.0](https://github.com/substrait-io/substrait-java/compare/v0.71.0...v0.72.0) (2025-12-07)

### Features

* support Nested Structs ([#579](https://github.com/substrait-io/substrait-java/issues/579)) ([8845bbf](https://github.com/substrait-io/substrait-java/commit/8845bbff6954c2ba5105469401a94bf54c686f49))

## [0.71.0](https://github.com/substrait-io/substrait-java/compare/v0.70.0...v0.71.0) (2025-11-30)

### Features

* add ExchangeRel support to core ([#602](https://github.com/substrait-io/substrait-java/issues/602)) ([194f4e4](https://github.com/substrait-io/substrait-java/commit/194f4e4a31c0c81fdcd88431e6e008e72e3909b3))
* **isthmus:** mapping of string extract and pad fns ([#619](https://github.com/substrait-io/substrait-java/issues/619)) ([359cc7c](https://github.com/substrait-io/substrait-java/commit/359cc7ccadf92522c6b8cae8d0886e28c228586b))

### Bug Fixes

* add missing serialVersionUIDs ([#621](https://github.com/substrait-io/substrait-java/issues/621)) ([2e3ca32](https://github.com/substrait-io/substrait-java/commit/2e3ca32efc5e770d2ae4d584179353f07e75e0ef))

## [0.70.0](https://github.com/substrait-io/substrait-java/compare/v0.69.0...v0.70.0) (2025-11-23)

### Features

* **isthmus:** mapping of positional scalar fns ([#610](https://github.com/substrait-io/substrait-java/issues/610)) ([96541a9](https://github.com/substrait-io/substrait-java/commit/96541a9f2cc0cc0c38215b0a31473246732347c3))
* **isthmus:** support bitwise left shift ([#605](https://github.com/substrait-io/substrait-java/issues/605)) ([22448d1](https://github.com/substrait-io/substrait-java/commit/22448d1bd36c5face8675e31b85d5378d40901a0))

## [0.69.0](https://github.com/substrait-io/substrait-java/compare/v0.68.0...v0.69.0) (2025-11-16)

### Features

* enforce row type consistency with schema in virtual tables ([#601](https://github.com/substrait-io/substrait-java/issues/601)) ([a5322ea](https://github.com/substrait-io/substrait-java/commit/a5322eadfab67bfcdd34453fd94c9a559ccc762c))
* **isthmus:** log2, greatest and least scalar fn mappings added ([#598](https://github.com/substrait-io/substrait-java/issues/598)) ([848bfcf](https://github.com/substrait-io/substrait-java/commit/848bfcf83f4235574a686a4d48407a8f8ba5d99e))
* **isthmus:** true, false, distinct comparator scalar fn mappings ([#597](https://github.com/substrait-io/substrait-java/issues/597)) ([f59ecc5](https://github.com/substrait-io/substrait-java/commit/f59ecc5ad78eb8f8e79c664fbb4f81c572bb9aa1))

### Bug Fixes

* handle valid YAML extension types ([#595](https://github.com/substrait-io/substrait-java/issues/595)) ([ed25bed](https://github.com/substrait-io/substrait-java/commit/ed25bed9233b1bd393f128c580cf1ab7fbcca0b9)), closes [#594](https://github.com/substrait-io/substrait-java/issues/594)
* **isthmus:** support TPC-DS queries 1, 30, 81 ([#593](https://github.com/substrait-io/substrait-java/issues/593)) ([442f1f2](https://github.com/substrait-io/substrait-java/commit/442f1f2b171d8eb691f3dcc01c2d6fe03c6add8a))

## [0.68.0](https://github.com/substrait-io/substrait-java/compare/v0.67.0...v0.68.0) (2025-11-02)

### Features

* **isthmus:** mapping of angular scalar functions ([#586](https://github.com/substrait-io/substrait-java/issues/586)) ([9c4db92](https://github.com/substrait-io/substrait-java/commit/9c4db9207cec12a7cf1625ee29343ba402c864d6))
* **isthmus:** mapping of bitwise functions from substrait ([#582](https://github.com/substrait-io/substrait-java/issues/582)) ([e1e8689](https://github.com/substrait-io/substrait-java/commit/e1e86891369ee69be302a9c2eb3405395acee9f4))
* **isthmus:** mapping of factorial function from substrait ([#589](https://github.com/substrait-io/substrait-java/issues/589)) ([4b83666](https://github.com/substrait-io/substrait-java/commit/4b8366675108d506fd29f1cd355bd27712cafe73))
* **isthmus:** mapping of hyperbolic functions from substrait ([#581](https://github.com/substrait-io/substrait-java/issues/581)) ([ac9ad28](https://github.com/substrait-io/substrait-java/commit/ac9ad2839feb77288bee4d910133f89ef0324a16))
* **isthmus:** support fully qualified table names in SubstraitCreateStatementParser ([#575](https://github.com/substrait-io/substrait-java/issues/575)) ([ab5943e](https://github.com/substrait-io/substrait-java/commit/ab5943e53f73ed73f816f2c3f59502378b3bf15c))

## [0.67.0](https://github.com/substrait-io/substrait-java/compare/v0.66.0...v0.67.0) (2025-10-28)

### Features

* **core,isthmus:** support grouping set index in Aggregate ([#565](https://github.com/substrait-io/substrait-java/issues/565)) ([a00a811](https://github.com/substrait-io/substrait-java/commit/a00a811bb2043151b242169e92a65969b93bf5a4))
* **isthmus:** extend Schema collector for dml ([#570](https://github.com/substrait-io/substrait-java/issues/570)) ([3f0f8f1](https://github.com/substrait-io/substrait-java/commit/3f0f8f14cc8483250ced8a4a5293cf21e1ce85dd))
* **isthmus:** improve extensibility of SubstraitRelVisitor ([#553](https://github.com/substrait-io/substrait-java/issues/553)) ([3c3db23](https://github.com/substrait-io/substrait-java/commit/3c3db23b7c508cd50a6317a9970c1d0fe7b0ec16))
* **isthmus:** mapping of square root function from substrait to calcite ([#568](https://github.com/substrait-io/substrait-java/issues/568)) ([a140d21](https://github.com/substrait-io/substrait-java/commit/a140d21e88732f377effd61abd8e2a2958ce4d43))
* **spark:** dialect YAML file for spark converter ([#554](https://github.com/substrait-io/substrait-java/issues/554)) ([c4c6153](https://github.com/substrait-io/substrait-java/commit/c4c6153958182941a83b96beade193fd62652d81))

### Bug Fixes

* **core:** close AdvancedExtension serde gaps ([#569](https://github.com/substrait-io/substrait-java/issues/569)) ([2ba50cb](https://github.com/substrait-io/substrait-java/commit/2ba50cbf5f063817de6501e27346db6ea7a2b90c))
* **core:** disable @Generated annotations for immutables ([#560](https://github.com/substrait-io/substrait-java/issues/560)) ([7222906](https://github.com/substrait-io/substrait-java/commit/7222906351ecec1e8cf06085529fb4c53c1d25d0))
* prevents exception on construction of `FunctionConverter` with duplicate functions ([#564](https://github.com/substrait-io/substrait-java/issues/564)) ([1b1dc73](https://github.com/substrait-io/substrait-java/commit/1b1dc73f6bab54abfdb41f810b77eaa01ebb7b01)), closes [#562](https://github.com/substrait-io/substrait-java/issues/562)
* **spark:** add versions for jackson dependencies ([#576](https://github.com/substrait-io/substrait-java/issues/576)) ([3605322](https://github.com/substrait-io/substrait-java/commit/36053226839f3943faa7e4c5a704c3b35574a0bd))
* **spark:** convert UnsafeArrayData literal to substrait ([#557](https://github.com/substrait-io/substrait-java/issues/557)) ([a6b2187](https://github.com/substrait-io/substrait-java/commit/a6b21874b24431699d75d9b22d528d7b02e3fd9a))

## [0.66.0](https://github.com/substrait-io/substrait-java/compare/v0.65.0...v0.66.0) (2025-10-09)

### ⚠ BREAKING CHANGES

* **core:** Plan now uses POJO AdvancedExtension
* **core:** Optimization class is now nested under AdvancedExtension
* **core:** Enhancement class is now nested under AdvancedExtension
* **core:** EmptyOptimization has been removed
* uri() methods are now urn()
* namespace() methods are now urn()
* removed SimpleExtension.loadDefaults

### Features

* **core:** use AdvancedExtension POJO in Plan POJO ([#533](https://github.com/substrait-io/substrait-java/issues/533)) ([2819bc6](https://github.com/substrait-io/substrait-java/commit/2819bc681f5b4d14bb54f4de83d4e0065a4ea3db))
* enable handling of URNs alongside URIs ([#522](https://github.com/substrait-io/substrait-java/issues/522)) ([26e1e3e](https://github.com/substrait-io/substrait-java/commit/26e1e3e43b3ce63a4893707f13579ee42bb97025))
* handle new grouping mechanism in AggregateRel protos ([#521](https://github.com/substrait-io/substrait-java/issues/521)) ([637ffbf](https://github.com/substrait-io/substrait-java/commit/637ffbfbdf5cb230fd60932a6aa7e2d235c74445))
* introduce DefaultExtensionCatalog.DEFAULT_COLLECTION ([#529](https://github.com/substrait-io/substrait-java/issues/529)) ([a15d9c7](https://github.com/substrait-io/substrait-java/commit/a15d9c7608808da9ad54e40fca0f625b3db1adba))
* **isthmus:** support subquery nested in project ([#530](https://github.com/substrait-io/substrait-java/issues/530)) ([c7a1a34](https://github.com/substrait-io/substrait-java/commit/c7a1a34686c8227fe6f9b92cdf9430c73868e9b6))
* **spark:** support Hive DDL / Insert operations ([#518](https://github.com/substrait-io/substrait-java/issues/518)) ([c20836e](https://github.com/substrait-io/substrait-java/commit/c20836e1b5eef7dcc569b60466f33f49471d70ea))

### Bug Fixes

* **isthmus:** improve derivation of AggregateRel return type ([#508](https://github.com/substrait-io/substrait-java/issues/508)) ([cf4ad93](https://github.com/substrait-io/substrait-java/commit/cf4ad93df23f2df5b4252b28b663e76a27406989))
* **spark:** sporadic failures in Hive test suite ([#540](https://github.com/substrait-io/substrait-java/issues/540)) ([c6c552d](https://github.com/substrait-io/substrait-java/commit/c6c552dc84d43877a37954b393b6f475fce0b53c))

## [0.65.0](https://github.com/substrait-io/substrait-java/compare/v0.64.0...v0.65.0) (2025-09-22)

### Features

* **isthmus:** introduce SqlToSubstrait[#convert](https://github.com/substrait-io/substrait-java/issues/convert) method ([#473](https://github.com/substrait-io/substrait-java/issues/473)) ([418742f](https://github.com/substrait-io/substrait-java/commit/418742f81135d5ac6347289aae008e0a54720cef))
* **isthmus:** introduce SubstraitSqlToCalcite and SubstraitStatementParser ([#474](https://github.com/substrait-io/substrait-java/issues/474)) ([8906eb8](https://github.com/substrait-io/substrait-java/commit/8906eb87a826da3188757b296bebbf83a6266bf8))
* **isthmus:** support limited processing of DDL statements([#432](https://github.com/substrait-io/substrait-java/issues/432)) ([260a1c4](https://github.com/substrait-io/substrait-java/commit/260a1c42f7d1ffc771ee50b481972ca95ee3e4e4))

### Bug Fixes

* **isthmus:** tpcds q67 ([#503](https://github.com/substrait-io/substrait-java/issues/503)) ([77e7f8f](https://github.com/substrait-io/substrait-java/commit/77e7f8ff58286dedf11c30c0605b8c2ed2c49acf))

### Reverts

* remove incorrect override of Project output schema ([9c0248a](https://github.com/substrait-io/substrait-java/commit/9c0248afa987440c185c34eb99c6f35f8c073cfd))

## [0.64.0](https://github.com/substrait-io/substrait-java/compare/v0.63.0...v0.64.0) (2025-08-11)

### Features

* **core:** handle all Hint fields ([#469](https://github.com/substrait-io/substrait-java/issues/469)) ([3dfe665](https://github.com/substrait-io/substrait-java/commit/3dfe665b421a832071de99d2a1bd996b53641583))

### Bug Fixes

* **isthmus-cli:** use correct version in Isthmus CLI help ([#456](https://github.com/substrait-io/substrait-java/issues/456)) ([d61d8c4](https://github.com/substrait-io/substrait-java/commit/d61d8c44b252ab2653ac282fa4266717bc0a7e4d))

## [0.63.0](https://github.com/substrait-io/substrait-java/compare/v0.62.1...v0.63.0) (2025-08-03)

### Features

* **core,isthmus:** add DML support to SqlToSubstrait ([#431](https://github.com/substrait-io/substrait-java/issues/431)) ([76684d8](https://github.com/substrait-io/substrait-java/commit/76684d83f7bdc5295f6370c73ef2cb79acc927dd))
* **spark:** add LogicalRDD support ([#451](https://github.com/substrait-io/substrait-java/issues/451)) ([142c574](https://github.com/substrait-io/substrait-java/commit/142c5749989532cc5198075c87ac1fedd08d60ab))
* **spark:** support insert/append operations ([#429](https://github.com/substrait-io/substrait-java/issues/429)) ([1954fc8](https://github.com/substrait-io/substrait-java/commit/1954fc8df5fbd7366b0dcac7205ab69454a5843b))

### Bug Fixes

* **isthmus:** handle subqueries with outer field references ([#426](https://github.com/substrait-io/substrait-java/issues/426)) ([7cf1ccf](https://github.com/substrait-io/substrait-java/commit/7cf1ccff7abc249324f6bd52062d586087c94a3c))

## [0.62.1](https://github.com/substrait-io/substrait-java/compare/v0.62.0...v0.62.1) (2025-07-27)

### Bug Fixes

* **isthmus-cli:** fix tpch_smoke.sh test script ([#450](https://github.com/substrait-io/substrait-java/issues/450)) ([7906dae](https://github.com/substrait-io/substrait-java/commit/7906dae271faccccd11cd0d957c8ae9e7c4d0488))

## [0.62.0](https://github.com/substrait-io/substrait-java/compare/v0.61.0...v0.62.0) (2025-07-13)

### ⚠ BREAKING CHANGES

* ExpressionVisitor methods have new Context parameter
* RelVisitor methods have new Context parameter
* FunArgVisitor methods have new Context parameter
* **isthmus:** removed SqlToSubstrait#execute(String sql, List tables)
* **isthmus:** removed SqlToSubstrait#execute(String sql, String name, Schema schema)
* **isthmus:** removed SubstraitToSql#substraitRelToCalciteRel(Rel relRoot, List tables)
* **isthmus:** removed SubstraitToSql#toSql(RelNode root)

### Features

* add visit context to Rel, Expression and FuncArg Visitors ([#427](https://github.com/substrait-io/substrait-java/issues/427)) ([0e9c492](https://github.com/substrait-io/substrait-java/commit/0e9c4926a6e8341df07d541fd2a451d3c2117f81))
* **isthmus:** additional parsing and unparsing utils for SQL ([#430](https://github.com/substrait-io/substrait-java/issues/430)) ([f87929d](https://github.com/substrait-io/substrait-java/commit/f87929d00775e767b69de782e52859b9c2499b68))

## [0.61.0](https://github.com/substrait-io/substrait-java/compare/v0.60.0...v0.61.0) (2025-06-29)

### Features

* add additional builder methods  ([#425](https://github.com/substrait-io/substrait-java/issues/425)) ([bb4c3bd](https://github.com/substrait-io/substrait-java/commit/bb4c3bd2e260537488e7212cc370ae968dbabf5a))
* **core:** add UpdateRel ([#421](https://github.com/substrait-io/substrait-java/issues/421)) ([5e6741d](https://github.com/substrait-io/substrait-java/commit/5e6741d8059f632432eb5722026c562f881ff873))
* **isthmus:** support for SQL TRIM function ([#401](https://github.com/substrait-io/substrait-java/issues/401)) ([1f0e4de](https://github.com/substrait-io/substrait-java/commit/1f0e4ded677ced7cb50ec346b996c6abf2ac3280))
* **spark:** use core spec bitwise shift functions ([#424](https://github.com/substrait-io/substrait-java/issues/424)) ([8f14f62](https://github.com/substrait-io/substrait-java/commit/8f14f625a15178bb0440080d5ae136e5ca242a4f))

### Bug Fixes

* **core:** convert hints in ProtoRelConverter ([#420](https://github.com/substrait-io/substrait-java/issues/420)) ([e0a0fda](https://github.com/substrait-io/substrait-java/commit/e0a0fdaf49e9583bbd85af30889a43ea0051e0d5))

## [0.60.0](https://github.com/substrait-io/substrait-java/compare/v0.59.0...v0.60.0) (2025-06-18)

### Features

* **core:** add DdlRel ([#410](https://github.com/substrait-io/substrait-java/issues/410)) ([18a12d5](https://github.com/substrait-io/substrait-java/commit/18a12d5b8c14ee3101c34af2211104e462e9ffb2))

## [0.59.0](https://github.com/substrait-io/substrait-java/compare/v0.58.0...v0.59.0) (2025-06-15)

### Features

* expose Nullability and Decomposability enums ([#415](https://github.com/substrait-io/substrait-java/issues/415)) ([e445a50](https://github.com/substrait-io/substrait-java/commit/e445a504d807eaff687e06b7aac1d404ea7735a0))
* support Version field in plans ([#406](https://github.com/substrait-io/substrait-java/issues/406)) ([de8bf2a](https://github.com/substrait-io/substrait-java/commit/de8bf2abce302dadbcead2364f6f950fc5ad6673))

### Bug Fixes

* add fallback to version lookup ([#413](https://github.com/substrait-io/substrait-java/issues/413)) ([ac707e2](https://github.com/substrait-io/substrait-java/commit/ac707e2738df9465685b8214a9e53e01415971f3))
* **core:** create and use SubstraitVersion Java class ([#416](https://github.com/substrait-io/substrait-java/issues/416)) ([1cb2eec](https://github.com/substrait-io/substrait-java/commit/1cb2eec1e61ce645fb8bcfe3a8cce5e62ecebaf0))
* **isthmus,spark:** add MANIFEST.MF specification version to JAR ([#412](https://github.com/substrait-io/substrait-java/issues/412)) ([33c83dc](https://github.com/substrait-io/substrait-java/commit/33c83dc2babb4114607333d8299eced88e4afa01))

## [0.58.0](https://github.com/substrait-io/substrait-java/compare/v0.57.0...v0.58.0) (2025-06-08)

### Features

* add new join types ([#402](https://github.com/substrait-io/substrait-java/issues/402)) ([5907815](https://github.com/substrait-io/substrait-java/commit/59078154160a08e356276d16c5c50739325cc4ee))
* **core:** add WriteRel ([#404](https://github.com/substrait-io/substrait-java/issues/404)) ([e0cd29f](https://github.com/substrait-io/substrait-java/commit/e0cd29f8f0ece0781fa94f1473e32ce93ddc9944))
* **isthmus:** upgrade Apache Calcite to 1.40.0 ([#403](https://github.com/substrait-io/substrait-java/issues/403)) ([bbe77ef](https://github.com/substrait-io/substrait-java/commit/bbe77efdae98738a16ecdff6d40c990a1061de79))

### Bug Fixes

* **core:** start functionAnchors at 1 instead of 0 ([#408](https://github.com/substrait-io/substrait-java/issues/408)) ([007accf](https://github.com/substrait-io/substrait-java/commit/007accf75f71576cc91cd8d52e97fab771b5cfd1))

## [0.57.0](https://github.com/substrait-io/substrait-java/compare/v0.56.0...v0.57.0) (2025-05-25)

### Features

* **spark:** add support for trim functions ([#400](https://github.com/substrait-io/substrait-java/issues/400)) ([ea961a0](https://github.com/substrait-io/substrait-java/commit/ea961a0859bc93c777a89e2db38851de0058ce4f))

## [0.56.0](https://github.com/substrait-io/substrait-java/compare/v0.55.0...v0.56.0) (2025-05-04)

### ⚠ BREAKING CHANGES

* LookupCalciteSchema has been removed

### Features

* introduce SchemaCollector ([#391](https://github.com/substrait-io/substrait-java/issues/391)) ([5d3abd8](https://github.com/substrait-io/substrait-java/commit/5d3abd8ede1d694d9c9d50e5ad6c581cc7fcc97f))
* **isthmus:** upgrade to Calcite 1.39.0 ([#393](https://github.com/substrait-io/substrait-java/issues/393)) ([4d3640d](https://github.com/substrait-io/substrait-java/commit/4d3640d52291ea2dec2bfbf76f8f9f972ab98a7f))

## [0.55.0](https://github.com/substrait-io/substrait-java/compare/v0.54.1...v0.55.0) (2025-04-27)

### Features

* **isthmus:** enable CHAR upcasting in Calcite function calls ([#338](https://github.com/substrait-io/substrait-java/issues/338)) ([a1fbe53](https://github.com/substrait-io/substrait-java/commit/a1fbe53be4a3dd29ac20f061e9cb30fbe728c010))

## [0.54.1](https://github.com/substrait-io/substrait-java/compare/v0.54.0...v0.54.1) (2025-04-20)

### Bug Fixes

* broken link in readme.md ([#389](https://github.com/substrait-io/substrait-java/issues/389)) ([5d8e148](https://github.com/substrait-io/substrait-java/commit/5d8e148b180a26cb4a4f24b6425cac35db2ae41b))

## [0.54.0](https://github.com/substrait-io/substrait-java/compare/v0.53.0...v0.54.0) (2025-04-13)

### ⚠ BREAKING CHANGES

* **isthmus:** converting a Calcite RelRoot no longer produces a Substrait Rel

### Features

* **isthmus:** convert Calcite RelRoot to Substrait Plan.Root ([#370](https://github.com/substrait-io/substrait-java/issues/370)) ([b7abddd](https://github.com/substrait-io/substrait-java/commit/b7abddd4f05d9d45a37f163f398c611f39228f87))
* **isthmus:** support more datetime extract variants ([#360](https://github.com/substrait-io/substrait-java/issues/360)) ([134c224](https://github.com/substrait-io/substrait-java/commit/134c22400003d7eea71c701344a9e4c8dedb9ba8))
* **pojo:** add builder methods to Plan and Plan.Root ([#374](https://github.com/substrait-io/substrait-java/issues/374)) ([ae70dc4](https://github.com/substrait-io/substrait-java/commit/ae70dc4fe62813b30059e61015cf94231ba937b6))
* **spark:** add some date functions ([#373](https://github.com/substrait-io/substrait-java/issues/373)) ([2ece486](https://github.com/substrait-io/substrait-java/commit/2ece486585845f518af9f5317c4df3a543cfa9ed))
* **spark:** make SparkSession optional ([#385](https://github.com/substrait-io/substrait-java/issues/385)) ([b67599e](https://github.com/substrait-io/substrait-java/commit/b67599ebd558b84f3249c0cf7d7ec0a733cceb76))

### Bug Fixes

* **isthmus:** use explicit return type for scalar function expressions ([#355](https://github.com/substrait-io/substrait-java/issues/355)) ([867697c](https://github.com/substrait-io/substrait-java/commit/867697c030410a0334800e9c107733078f9c4805))
* **spark:** remove internal functions MakeDecimal and UnscaledValue ([#386](https://github.com/substrait-io/substrait-java/issues/386)) ([7a689e9](https://github.com/substrait-io/substrait-java/commit/7a689e906d7579bff6dd10797796855adaa2e989))
* **spark:** use ImmutableFileFormat builders ([#384](https://github.com/substrait-io/substrait-java/issues/384)) ([7b520a7](https://github.com/substrait-io/substrait-java/commit/7b520a77ad1798effadb014ce9791c8e4ebd76da))

## [0.53.0](https://github.com/substrait-io/substrait-java/compare/v0.52.0...v0.53.0) (2025-04-06)

### ⚠ BREAKING CHANGES

* **isthmus:** removed AllowsSqlBatch from FeatureBoard
* **isthmus:** removed SqlConformance from FeatureBoard
* **isthmus:** removed CrossJoinPolicy from FeatureBoard
* removed function-based table lookup conversion methods

### Features

* **isthmus:** always allow batch queries ([#372](https://github.com/substrait-io/substrait-java/issues/372)) ([2d17d58](https://github.com/substrait-io/substrait-java/commit/2d17d58d8ba4c2ae089f0c0acb1e6ab5d2de54e9))
* **isthmus:** always emit Cross relation when possible ([#367](https://github.com/substrait-io/substrait-java/issues/367)) ([7705243](https://github.com/substrait-io/substrait-java/commit/77052432237bc1d529a49f8b05b7577c854b93bc))
* **isthmus:** parse SQL using SqlConformanceEnum.LENIENT  ([#368](https://github.com/substrait-io/substrait-java/issues/368)) ([2ce3501](https://github.com/substrait-io/substrait-java/commit/2ce3501c516bdc8021d772983cbaba12ff88a2f1))
* new Prepare.CatalogReader based APIs for SQL to/from Substrait ([#363](https://github.com/substrait-io/substrait-java/issues/363)) ([3852640](https://github.com/substrait-io/substrait-java/commit/385264096cd6f33997e53bf64d6b1bcbcff1fec3))

### Bug Fixes

* missing throws for CREATE statement conversion failures ([#364](https://github.com/substrait-io/substrait-java/issues/364)) ([339b59c](https://github.com/substrait-io/substrait-java/commit/339b59ce9dd348bb6ddbc770f40582bc09b61958))
* **spark:** enable aliased expressions to round-trip ([#348](https://github.com/substrait-io/substrait-java/issues/348)) ([791f7ce](https://github.com/substrait-io/substrait-java/commit/791f7ceefbaa070678e980271693590bb680ef02))

## [0.52.0](https://github.com/substrait-io/substrait-java/compare/v0.51.0...v0.52.0) (2025-03-30)

### ⚠ BREAKING CHANGES

* **spark:** root names for plans now include nested names
* type derivation for Set now matches spec
* type derivation for Join now handles ANTI and SEMI
* validate and refine ScalarSubquery return type during proto conversion
* set nullability of aggregate references and groupings coming from Spark

### Features

* add `unquotedcasing` option to Isthmus CLI ([#351](https://github.com/substrait-io/substrait-java/issues/351)) ([f859c2e](https://github.com/substrait-io/substrait-java/commit/f859c2e9f64cd9ff8873b06aa4e45bde8b7071ae))
* **isthmus:** add support for scalar subqueries ([#353](https://github.com/substrait-io/substrait-java/issues/353)) ([5b12f9b](https://github.com/substrait-io/substrait-java/commit/5b12f9b42852424f68d90ffc8a24b506ac5ab2ae))
* **isthmus:** support exists and unique set predicates ([#354](https://github.com/substrait-io/substrait-java/issues/354)) ([424e8b3](https://github.com/substrait-io/substrait-java/commit/424e8b3ced43917ef3c43441eca8d081a3e80d0e))
* **spark:** support for Struct types and literals ([#342](https://github.com/substrait-io/substrait-java/issues/342)) ([f27004a](https://github.com/substrait-io/substrait-java/commit/f27004af97381f04faa2c921092534b88120f870))
* **spark:** support merged structures ([#346](https://github.com/substrait-io/substrait-java/issues/346)) ([9e4afb9](https://github.com/substrait-io/substrait-java/commit/9e4afb9dc0be63a77f73b3270ef02264f4cee15f))

### Bug Fixes

* added protobuf roundtrip testing to Spark and fixed surfaced issued ([#315](https://github.com/substrait-io/substrait-java/issues/315)) ([fd74922](https://github.com/substrait-io/substrait-java/commit/fd74922e0cf2b2c1643b9bc2920e836c8559f1e2))

## [0.51.0](https://github.com/substrait-io/substrait-java/compare/v0.50.0...v0.51.0) (2025-03-23)

### ⚠ BREAKING CHANGES

* parsed SQL no longer uses the default Calcite type system
* ToProto#toProto now consumes a RelProtoConverter

build: enable protobuf compilation for use with Isthmus tests
test: add RelExtensionRoundtripTest

Helps verify that substrait-java can handle Extension relations from
protobuf to Calcite and back

### Features

* allow for more ergonomic conversion of Extension relations ([#341](https://github.com/substrait-io/substrait-java/issues/341)) ([d705f5a](https://github.com/substrait-io/substrait-java/commit/d705f5afb56cef28221aed0904da55541e3c2a6e))
* **spark:** support round, floor and ceil functions ([#347](https://github.com/substrait-io/substrait-java/issues/347)) ([2d7fd37](https://github.com/substrait-io/substrait-java/commit/2d7fd37b39c3be4366d48e7eeadc30644645f2ed))
* **spark:** support SELECT without FROM clause ([#344](https://github.com/substrait-io/substrait-java/issues/344)) ([d91b44f](https://github.com/substrait-io/substrait-java/commit/d91b44fd7c7fbea7f75046f188eb63ada6d77a7a))

### Bug Fixes

* use SubstraitTypeSystem when parsing SQL ([#350](https://github.com/substrait-io/substrait-java/issues/350)) ([128d497](https://github.com/substrait-io/substrait-java/commit/128d497ed87ad7cd45cc96e5eeb5fa73870b7b65))

## [0.50.0](https://github.com/substrait-io/substrait-java/compare/v0.49.0...v0.50.0) (2025-03-16)

### Features

* **isthmus:** convert Substrait EmptyScan to Calcite Values relation ([#340](https://github.com/substrait-io/substrait-java/issues/340)) ([1ec0c57](https://github.com/substrait-io/substrait-java/commit/1ec0c577ea3f4dad32ac15301cf237173796d47b))
* **isthmus:** support converting Substrait Plan.Root to Calcite RelRoot ([#339](https://github.com/substrait-io/substrait-java/issues/339)) ([42b87ae](https://github.com/substrait-io/substrait-java/commit/42b87ae885e809404204a166d2addb5e4780150a))
* **spark:** support ExistenceJoin internal join type ([#333](https://github.com/substrait-io/substrait-java/issues/333)) ([59270f2](https://github.com/substrait-io/substrait-java/commit/59270f2abf92e2b2362f583238eddafb25e5ef91))

### Bug Fixes

* **isthmus:** sql day/time interval conversion ([#335](https://github.com/substrait-io/substrait-java/issues/335)) ([9900782](https://github.com/substrait-io/substrait-java/commit/990078222f8a45e55cb4d2f9a4d4253d20ee5621))

## [0.49.0](https://github.com/substrait-io/substrait-java/compare/v0.48.0...v0.49.0) (2025-03-09)

### Features

* **pojo:** support best_effort_filter on ReadRel messages ([780a0bb](https://github.com/substrait-io/substrait-java/commit/780a0bb22a6cc625cd118eeb59a5f013bc468ba0))

## [0.48.0](https://github.com/substrait-io/substrait-java/compare/v0.47.0...v0.48.0) (2025-02-24)

### Features

* **isthmus:** support full expressions in SortRel ([#322](https://github.com/substrait-io/substrait-java/issues/322)) ([80f8678](https://github.com/substrait-io/substrait-java/commit/80f867891450f8ecb24c9e363ff6d161fc0e5b0b))

## [0.47.0](https://github.com/substrait-io/substrait-java/compare/v0.46.1...v0.47.0) (2025-02-09)

### ⚠ BREAKING CHANGES

* **pojo:** AggregationPhase enum has a new allowed value

### Features

* **pojo:** add UNSPECIFIED value to AggregationPhase enum ([#320](https://github.com/substrait-io/substrait-java/issues/320)) ([a0ef1dd](https://github.com/substrait-io/substrait-java/commit/a0ef1dd2cd79df43dbecf4ec28f64fa8c7216993))
* **spark:** add support for DelimiterSeparatedTextReadOptions ([#323](https://github.com/substrait-io/substrait-java/issues/323)) ([13da183](https://github.com/substrait-io/substrait-java/commit/13da1839dfb75f09e5fe7b61b7301efca5bcbef6))

### Bug Fixes

* **isthmus:** more standard SQL for TPCDS query 72 ([#326](https://github.com/substrait-io/substrait-java/issues/326)) ([b4afdb2](https://github.com/substrait-io/substrait-java/commit/b4afdb25364eefa2094c7d86c2c19581a8054e1b))

## [0.46.1](https://github.com/substrait-io/substrait-java/compare/v0.46.0...v0.46.1) (2024-12-22)

### Bug Fixes

* **spark:** casting date/time requires timezone ([#318](https://github.com/substrait-io/substrait-java/issues/318)) ([af5a615](https://github.com/substrait-io/substrait-java/commit/af5a6153892b474ada1876a85fd2c203ba8fe90a))

## [0.46.0](https://github.com/substrait-io/substrait-java/compare/v0.45.1...v0.46.0) (2024-12-01)

### Features

* **spark:** add some numeric function mappings ([#317](https://github.com/substrait-io/substrait-java/issues/317)) ([6bb46ac](https://github.com/substrait-io/substrait-java/commit/6bb46ac46ba63fc38d15b8913fb3b3b09f9939b3))

## [0.45.1](https://github.com/substrait-io/substrait-java/compare/v0.45.0...v0.45.1) (2024-11-24)

### Bug Fixes

* **spark:** incorrect conversion of expand relation ([#316](https://github.com/substrait-io/substrait-java/issues/316)) ([6c78d48](https://github.com/substrait-io/substrait-java/commit/6c78d48126859e72a402b4bdab45c877db77ae18))

## [0.45.0](https://github.com/substrait-io/substrait-java/compare/v0.44.0...v0.45.0) (2024-10-27)

### ⚠ BREAKING CHANGES

* **spark:** Spark TimestampNTZType is now emitted as Substrait PrecisionTimestamp
* **spark:** Spark TimestampType is now emitted as Substrait PrecisionTimestampTZ

feat(core): added support for Expression.EmptyMapLiteral

### Features

* **spark:** add Window support ([#307](https://github.com/substrait-io/substrait-java/issues/307)) ([b3f61a2](https://github.com/substrait-io/substrait-java/commit/b3f61a20606b20e950e62180c9b140b31c82414d))
* **spark:** additional type and literal support ([#311](https://github.com/substrait-io/substrait-java/issues/311)) ([513a049](https://github.com/substrait-io/substrait-java/commit/513a04951e2fd3c2bf6767ad2eedb913df8d893e))
* **spark:** bitwise functions ([#309](https://github.com/substrait-io/substrait-java/issues/309)) ([b8ccd8b](https://github.com/substrait-io/substrait-java/commit/b8ccd8b0af4f95d6a27d8512607ee912d249df0a))
* **spark:** convert VirtualTableScan to LocalRelation ([#312](https://github.com/substrait-io/substrait-java/issues/312)) ([3f2cc1e](https://github.com/substrait-io/substrait-java/commit/3f2cc1e3bb136251c794999d3592690e00b31ddb))
* **spark:** enable upper/lower/concat/coalesce string functions ([#308](https://github.com/substrait-io/substrait-java/issues/308)) ([fc8a764](https://github.com/substrait-io/substrait-java/commit/fc8a764f2f18ef7ae33eed86102b3f7d1bbb1b90))

### Bug Fixes

* propagate sorts in aggregate function invocation proto->rel ([#313](https://github.com/substrait-io/substrait-java/issues/313)) ([75ebac2](https://github.com/substrait-io/substrait-java/commit/75ebac26290cc5ebee5cde4c59d3a829a2f5c05b))
* **spark:** nullability of output columns of expand relation ([#310](https://github.com/substrait-io/substrait-java/issues/310)) ([6413e55](https://github.com/substrait-io/substrait-java/commit/6413e55925f680d9a1b56b1213838ddd06c3a547))

## [0.44.0](https://github.com/substrait-io/substrait-java/compare/v0.43.0...v0.44.0) (2024-10-20)

### Features

* **spark:** map EqualNullSafe fn to is_not_distinct_from ([#306](https://github.com/substrait-io/substrait-java/issues/306)) ([357cc01](https://github.com/substrait-io/substrait-java/commit/357cc01a087cc7b6efe699d4921a688a11eeddb3))
* **spark:** support conversion of boolean types ([#305](https://github.com/substrait-io/substrait-java/issues/305)) ([d5452ad](https://github.com/substrait-io/substrait-java/commit/d5452add8815ab19d13a72a71305deefc06aff05))

## [0.43.0](https://github.com/substrait-io/substrait-java/compare/v0.42.0...v0.43.0) (2024-10-13)

### Features

* **spark:** support for FetchRel offset field ([#296](https://github.com/substrait-io/substrait-java/issues/296)) ([d22c07f](https://github.com/substrait-io/substrait-java/commit/d22c07f474bcc6d7894f455f8b4f723b313638a8))

## [0.42.0](https://github.com/substrait-io/substrait-java/compare/v0.41.0...v0.42.0) (2024-10-06)

### ⚠ BREAKING CHANGES

* EXCEPT ALL and INTERSECT ALL now output different SetOps

### Features

* **spark:** add MakeDecimal support ([#298](https://github.com/substrait-io/substrait-java/issues/298)) ([eec9727](https://github.com/substrait-io/substrait-java/commit/eec97274dc4b008335d59fc8d131de09ad818e67))
* **spark:** support UNION ALL in SparkSql ([#301](https://github.com/substrait-io/substrait-java/issues/301)) ([b55d8b0](https://github.com/substrait-io/substrait-java/commit/b55d8b04e007a112fff090b50db6b7c06c7bb63e))

### Miscellaneous Chores

* update to substrait v0.57.0 ([#300](https://github.com/substrait-io/substrait-java/issues/300)) ([eb484a3](https://github.com/substrait-io/substrait-java/commit/eb484a3037e50f6abfbb783aa36c4e9bb5449432))

## [0.41.0](https://github.com/substrait-io/substrait-java/compare/v0.40.0...v0.41.0) (2024-09-29)

### Features

* add ExpandRel support to core and Spark ([#295](https://github.com/substrait-io/substrait-java/issues/295)) ([32fea18](https://github.com/substrait-io/substrait-java/commit/32fea180e68ad7b0954e649a870e5e0ba8d7a6ed))

## [0.40.0](https://github.com/substrait-io/substrait-java/compare/v0.39.0...v0.40.0) (2024-09-22)

### ⚠ BREAKING CHANGES

* IntervalDay now has "subsecond" and "precision" fields instead
of "microseconds". Old protobufs should be still read correctly.

### Features

* support new IntervalCompound and updated IntervalDay types ([#288](https://github.com/substrait-io/substrait-java/issues/288)) ([e24ce6f](https://github.com/substrait-io/substrait-java/commit/e24ce6f3288f247c80f8162135ee4feddd705ff3))

## [0.39.0](https://github.com/substrait-io/substrait-java/compare/v0.38.1...v0.39.0) (2024-09-01)

### Features

* **isthmus:** injectable TypeConverter for window fn converters ([#291](https://github.com/substrait-io/substrait-java/issues/291)) ([d625648](https://github.com/substrait-io/substrait-java/commit/d6256487f75032e2e8fc803976d916e4a5bc44bd))

## [0.38.1](https://github.com/substrait-io/substrait-java/compare/v0.38.0...v0.38.1) (2024-08-18)

### Bug Fixes

* **core:** output nullability of IfThen depends on all possible outputs ([a0ca17b](https://github.com/substrait-io/substrait-java/commit/a0ca17bdb12aa7c3acbf9b5700fd546434bb2899))
* **core:** wrong type derivation for ConsistentPartitionWindow ([#286](https://github.com/substrait-io/substrait-java/issues/286)) ([60575b3](https://github.com/substrait-io/substrait-java/commit/60575b3ddc6f9abd7040ccc3d70be69bdf19403f))

## [0.38.0](https://github.com/substrait-io/substrait-java/compare/v0.37.0...v0.38.0) (2024-08-11)

### Features

* make ProtoRelConverter utility methods protected ([#285](https://github.com/substrait-io/substrait-java/issues/285)) ([3864710](https://github.com/substrait-io/substrait-java/commit/386471036d7cf93bd86a19b8511f20979aa7f23f))

## [0.37.0](https://github.com/substrait-io/substrait-java/compare/v0.36.0...v0.37.0) (2024-07-21)

### ⚠ BREAKING CHANGES

* AdvancedExtension#getOptimization() has been removed. Use getOptimizations() instead.

### Features

* literal support for precision timestamp types ([#283](https://github.com/substrait-io/substrait-java/issues/283)) ([94996f9](https://github.com/substrait-io/substrait-java/commit/94996f916478ed8141e5fb54b1c8411cc80f4abd))
* validate VirtualTableScan field names with schema ([#284](https://github.com/substrait-io/substrait-java/issues/284)) ([0f8514a](https://github.com/substrait-io/substrait-java/commit/0f8514a95f0ffa2c3cca645652ef91ebfd3ccb9d))

### Miscellaneous Chores

* update to substrait 0.52.0 ([#282](https://github.com/substrait-io/substrait-java/issues/282)) ([ada8d0b](https://github.com/substrait-io/substrait-java/commit/ada8d0be54b8bbd260b194c4e93f02ed42821b5d))

## [0.36.0](https://github.com/substrait-io/substrait-java/compare/v0.35.0...v0.36.0) (2024-07-14)

### ⚠ BREAKING CHANGES

* Expression#options now returns List<FunctionOption>
* ProtoAggregateFunctionConverter#from(AggregateFunction) now returns AggregateFunctionInvocation

### Bug Fixes

* include FunctionOptions when converting functions ([#278](https://github.com/substrait-io/substrait-java/issues/278)) ([e574913](https://github.com/substrait-io/substrait-java/commit/e57491333c7dd05ae3b1400e2185f807af1f5f88))

## [0.35.0](https://github.com/substrait-io/substrait-java/compare/v0.34.0...v0.35.0) (2024-06-30)

### Features

* deprecate Timestamp and TimestampTZ visit functions ([#273](https://github.com/substrait-io/substrait-java/issues/273)) ([8a8253e](https://github.com/substrait-io/substrait-java/commit/8a8253ec1f077b81d0da6503e299662048fca825))
* introduce substrait-spark module ([#271](https://github.com/substrait-io/substrait-java/issues/271)) ([8537dca](https://github.com/substrait-io/substrait-java/commit/8537dca93b410177f2ee5aefffe83f7c02a3668c))

## [0.34.0](https://github.com/substrait-io/substrait-java/compare/v0.33.0...v0.34.0) (2024-06-23)

### ⚠ BREAKING CHANGES

* getDfsNames() has been removed from VirtualTableScan
* getInitialSchema() not longer has a default implementation in VirtualTableScan

### Bug Fixes

* set VirtualTableScan schema explicitly ([#272](https://github.com/substrait-io/substrait-java/issues/272)) ([f1192cf](https://github.com/substrait-io/substrait-java/commit/f1192cfaf6c84fb1e466bae6eda75ba164444aa8))

## [0.33.0](https://github.com/substrait-io/substrait-java/compare/v0.32.0...v0.33.0) (2024-06-16)

### Features

* **isthmus:** support for PrecisionTimestamp conversions ([#262](https://github.com/substrait-io/substrait-java/issues/262)) ([e726904](https://github.com/substrait-io/substrait-java/commit/e72690425cb31e52bc37550c1c4851db1b927651))

### Bug Fixes

* **isthmus:** correct SLF4J dependency ([#268](https://github.com/substrait-io/substrait-java/issues/268)) ([3134504](https://github.com/substrait-io/substrait-java/commit/31345045d522bf85bc60a59d52e4dd55601abbf8))

## [0.32.0](https://github.com/substrait-io/substrait-java/compare/v0.31.0...v0.32.0) (2024-06-04)

### ⚠ BREAKING CHANGES

* Substrait FP32 is now mapped to Calcite REAL instead of FLOAT
* Calcite FLOAT is now mapped to Substrait FP64 instead of FP32

In Calcite, the Sql Type Names DOUBLE and FLOAT correspond to FP64, and REAL corresponds to FP32

### Bug Fixes

* account for struct fields in VirtualTableScan check ([#255](https://github.com/substrait-io/substrait-java/issues/255)) ([3bbcf82](https://github.com/substrait-io/substrait-java/commit/3bbcf82687bc51fdb1695436c198e91ba56befed))
* map Calcite REAL to Substrait FP32 ([#261](https://github.com/substrait-io/substrait-java/issues/261)) ([37331c2](https://github.com/substrait-io/substrait-java/commit/37331c2fbee679fd5ec482d8ff4d16f1c7c1c5c0))

## [0.31.0](https://github.com/substrait-io/substrait-java/compare/v0.30.0...v0.31.0) (2024-05-05)


### ⚠ BREAKING CHANGES

* **isthumus:** CLI related functionality is now in the io.substrait.isthmus.cli package

### Features

* allow deployment time selection of logging framework [#243](https://github.com/substrait-io/substrait-java/issues/243) ([#244](https://github.com/substrait-io/substrait-java/issues/244)) ([72bab63](https://github.com/substrait-io/substrait-java/commit/72bab63edf6c4ffb12c3c4b0e4f49d066e0c5524))
* **isthumus:** extract CLI into isthmus-cli project [#248](https://github.com/substrait-io/substrait-java/issues/248) ([#249](https://github.com/substrait-io/substrait-java/issues/249)) ([a49de62](https://github.com/substrait-io/substrait-java/commit/a49de62c670274cccfa8b94fb86e88b36fc716d3))

## [0.30.0](https://github.com/substrait-io/substrait-java/compare/v0.29.1...v0.30.0) (2024-04-28)


### ⚠ BREAKING CHANGES

* ParameterizedTypeVisitor has new visit methods
* TypeExpressionVisitor has new visit methods
* TypeVisitor has new visit methods
* BaseProtoTypes has new visit methods

### Bug Fixes

* handle FetchRels with only offset set ([#252](https://github.com/substrait-io/substrait-java/issues/252)) ([b334e1d](https://github.com/substrait-io/substrait-java/commit/b334e1d4004ebc4598cab7bc076f3d477e97a52a))


### Miscellaneous Chores

* update to substrait 0.48.0 ([#250](https://github.com/substrait-io/substrait-java/issues/250)) ([572fe57](https://github.com/substrait-io/substrait-java/commit/572fe57ccf473e3d680f8928dd5f6833583350cc))

## [0.29.1](https://github.com/substrait-io/substrait-java/compare/v0.29.0...v0.29.1) (2024-03-31)


### Bug Fixes

* correct function compound names for IntervalDay and IntervalYear [#240](https://github.com/substrait-io/substrait-java/issues/240) ([#242](https://github.com/substrait-io/substrait-java/issues/242)) ([856331b](https://github.com/substrait-io/substrait-java/commit/856331bae9901e618663622bbf60eaf923dea5b8))

## [0.29.0](https://github.com/substrait-io/substrait-java/compare/v0.28.1...v0.29.0) (2024-03-17)


### ⚠ BREAKING CHANGES

* **isthmus:** method ExpressionCreator.cast(Type, Expression) has been removed

### Features

* **isthmus:** support for safe casting ([#236](https://github.com/substrait-io/substrait-java/issues/236)) ([72785ad](https://github.com/substrait-io/substrait-java/commit/72785ad1a4bd1ba8481d75ddaf4f1a822bf9bf6b))

## [0.28.1](https://github.com/substrait-io/substrait-java/compare/v0.28.0...v0.28.1) (2024-03-10)


### Bug Fixes

* use coercive function matcher before least restrictive matcher ([#237](https://github.com/substrait-io/substrait-java/issues/237)) ([e7aa8ff](https://github.com/substrait-io/substrait-java/commit/e7aa8ff1fe11dd784074138bf75c1afa140b59db))

## [0.28.0](https://github.com/substrait-io/substrait-java/compare/v0.27.0...v0.28.0) (2024-02-25)


### Features

* **isthmus:** add WindowRelFunctionConverter ([#234](https://github.com/substrait-io/substrait-java/issues/234)) ([a5e1a21](https://github.com/substrait-io/substrait-java/commit/a5e1a219f273906fc068f1c3b91f6c59b87ca8ba))
* **isthmus:** improve signature matching for functions with wildcard arguments ([#226](https://github.com/substrait-io/substrait-java/issues/226)) ([ec1887c](https://github.com/substrait-io/substrait-java/commit/ec1887cf5778ff0f3cce50d03cfed43a46c9f2ba))


### Bug Fixes

* disable task caches in the gradle build ([#233](https://github.com/substrait-io/substrait-java/issues/233)) ([7879f2b](https://github.com/substrait-io/substrait-java/commit/7879f2b21832531f04a07cb9fcbb6e900f060c6c))
* **isthmus:** allow for conversion of plans containing Calcite SqlAggFunctions ([#230](https://github.com/substrait-io/substrait-java/issues/230)) ([0bdac49](https://github.com/substrait-io/substrait-java/commit/0bdac4949b26896e4c7da50b6362debacee365a1)), closes [#180](https://github.com/substrait-io/substrait-java/issues/180)

## [0.27.0](https://github.com/substrait-io/substrait-java/compare/v0.26.0...v0.27.0) (2024-02-18)


### ⚠ BREAKING CHANGES

* ExpressionVisitor now has a `visit(Expression.EmptyListLiteral)` method
* LiteralConstructorConverter constructor now requires a TypeConverter

### Features

* add support for empty list literals ([#227](https://github.com/substrait-io/substrait-java/issues/227)) ([2a98e3c](https://github.com/substrait-io/substrait-java/commit/2a98e3c97c3dad734bfe95a1846df36894513048))
* **pojo:** add POJO representation and converters for ConsistentPartitionWindowRel ([#231](https://github.com/substrait-io/substrait-java/issues/231)) ([f148bbb](https://github.com/substrait-io/substrait-java/commit/f148bbb8bb3f0feefa64d34f7644faf849f046e8))
* support for user-defined type literals ([#232](https://github.com/substrait-io/substrait-java/issues/232)) ([ca8187f](https://github.com/substrait-io/substrait-java/commit/ca8187ffd9be688b8aa9a3c45d8ab1b11e42a001))

## [0.26.0](https://github.com/substrait-io/substrait-java/compare/v0.25.0...v0.26.0) (2024-02-11)


### Features

* **isthmus:** support for replace ([#224](https://github.com/substrait-io/substrait-java/issues/224)) ([e96f4ba](https://github.com/substrait-io/substrait-java/commit/e96f4baa7e2aca179bda0eac1563297ef96a117c))
* **isthmus:** support for SQL expressions in CLI ([#209](https://github.com/substrait-io/substrait-java/issues/209)) ([e63388d](https://github.com/substrait-io/substrait-java/commit/e63388d1206edc894ab065b6ce13a8213d6cf066))


### Bug Fixes

* update bad URLs in the release readme ([#225](https://github.com/substrait-io/substrait-java/issues/225)) ([aad2739](https://github.com/substrait-io/substrait-java/commit/aad2739094620a63a8bf83d577c558d055bacaa3))

## [0.25.0](https://github.com/substrait-io/substrait-java/compare/v0.24.0...v0.25.0) (2024-01-21)


### ⚠ BREAKING CHANGES

* **isthmus:** signatures for aggregate building utils have changed

* feat: additional builder methods for arithmetic aggregate functions
* feat: sortField builder method
* feat: grouping builder method
* feat: add, subtract, multiply, divide and negate methods for builder
* refactor: extract row matching assertions to PlanTestBase
* feat(isthmus): improved Calcite support for Substrait Aggregate rels
* refactor: builder functions for aggregates and aggregate functions now
consume and return Aggregate.Measure instead of
AggregateFunctionInvocation

### Features

* enable conversion of SQL expressions to Substrait ExtendedExpressions ([#191](https://github.com/substrait-io/substrait-java/issues/191)) ([750220e](https://github.com/substrait-io/substrait-java/commit/750220e10b61d3bdff426e2ee4ee9a4ffcc71487))
* improved error messages for Substrait conversion failures ([#221](https://github.com/substrait-io/substrait-java/issues/221)) ([#222](https://github.com/substrait-io/substrait-java/issues/222)) ([8c70245](https://github.com/substrait-io/substrait-java/commit/8c70245d1f361005417a7184640d919eb6f36f77))
* **isthmus:** improved Calcite support for Substrait Aggregate rels  ([#214](https://github.com/substrait-io/substrait-java/issues/214)) ([1689c93](https://github.com/substrait-io/substrait-java/commit/1689c9348afc1116982c5e8a5c821f2590ca094e))

## [0.24.0](https://github.com/substrait-io/substrait-java/compare/v0.23.0...v0.24.0) (2024-01-07)


### Features

* introduce DefaultExtensionCatalog ([#217](https://github.com/substrait-io/substrait-java/issues/217)) ([35f9b62](https://github.com/substrait-io/substrait-java/commit/35f9b628ecc2c986c19dd5b8d50fc4a478d5a881))
* **isthmus:** additional output formats for cli ([#216](https://github.com/substrait-io/substrait-java/issues/216)) ([6e62f46](https://github.com/substrait-io/substrait-java/commit/6e62f46f69739ce31f0ed5cd3cdce11170d2cf57))

## [0.23.0](https://github.com/substrait-io/substrait-java/compare/v0.22.0...v0.23.0) (2023-12-18)


### Features

* **isthmus:** support for char_length ([#212](https://github.com/substrait-io/substrait-java/issues/212)) ([7424483](https://github.com/substrait-io/substrait-java/commit/7424483fc858f8fd2896d4a1cce0318ef83e50b0))
* **pojo:** support for extended expressions ([#206](https://github.com/substrait-io/substrait-java/issues/206)) ([9e023a7](https://github.com/substrait-io/substrait-java/commit/9e023a70a374541e949877dbe01609ea56dbfdae))

## [0.22.0](https://github.com/substrait-io/substrait-java/compare/v0.21.0...v0.22.0) (2023-11-26)


### Features

* **isthmus:** support inPredicate expression ([#205](https://github.com/substrait-io/substrait-java/issues/205)) ([133ab83](https://github.com/substrait-io/substrait-java/commit/133ab8335ccf57b50ddf7a12f7822c81613bc30a))

## [0.21.0](https://github.com/substrait-io/substrait-java/compare/v0.20.0...v0.21.0) (2023-11-19)


### Features

* add MergeJoinRel ([#201](https://github.com/substrait-io/substrait-java/issues/201)) ([237179f](https://github.com/substrait-io/substrait-java/commit/237179f9f170c30f34ed09a1810db19e3725a5bc))

## [0.20.0](https://github.com/substrait-io/substrait-java/compare/v0.19.0...v0.20.0) (2023-11-07)


### ⚠ BREAKING CHANGES

* RelCopyOnWriteVisitor now extends RelVisitor and has generic type parameter

### Features

* exhaustive copy on write visitors ([#199](https://github.com/substrait-io/substrait-java/issues/199)) ([39c56ab](https://github.com/substrait-io/substrait-java/commit/39c56ab36042fefd30e5cb35b68301ea135fee1a))
* improved ReadRel handling ([#194](https://github.com/substrait-io/substrait-java/issues/194)) ([6548670](https://github.com/substrait-io/substrait-java/commit/6548670cba32efd42b6d5a312a7793569dd3d531))
* initial NestedLoopJoin support ([#188](https://github.com/substrait-io/substrait-java/issues/188)) ([b66d5b1](https://github.com/substrait-io/substrait-java/commit/b66d5b1354841e86f7f6fd73fe077155559ecfe3))


### Bug Fixes

* map switch expression to a Calcite CASE statement ([#189](https://github.com/substrait-io/substrait-java/issues/189)) ([b938573](https://github.com/substrait-io/substrait-java/commit/b9385731bd384624f0d04771dead6262a7a9328e))

## [0.19.0](https://github.com/substrait-io/substrait-java/compare/v0.18.0...v0.19.0) (2023-10-29)


### Features

* initial HashJoinRel support  ([#187](https://github.com/substrait-io/substrait-java/issues/187)) ([46d03ca](https://github.com/substrait-io/substrait-java/commit/46d03ca240e4bd25f2cb892ffe4dc371844e298d))

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
