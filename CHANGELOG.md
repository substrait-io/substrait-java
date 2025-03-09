Release Notes
---

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
