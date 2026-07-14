# Customization

Every Isthmus converter — [`SqlToSubstrait`](sql-to-substrait.md),
[`SqlExpressionToSubstrait`](sql-expressions.md),
[`SubstraitToSql`](substrait-to-sql.md), and
[`SubstraitToCalcite`](substrait-to-calcite.md) — takes its configuration from a single
`ConverterProvider`. Customizing conversion behavior therefore means constructing (or
subclassing) a `ConverterProvider` and passing it to the converter.

## `ConverterProvider`

The no-argument constructor supplies system defaults: the standard Substrait extension
catalog (`DefaultExtensionCatalog.DEFAULT_COLLECTION`) and the Substrait type factory
(`SubstraitTypeSystem.TYPE_FACTORY`).

```java
// Defaults
ConverterProvider provider = new ConverterProvider();

// Custom extension collection (functions/types), default type factory
ConverterProvider withExtensions = new ConverterProvider(myExtensions);

// Full control: type factory, extensions, and the function/type converters
ConverterProvider full =
    new ConverterProvider(
        SubstraitTypeSystem.TYPE_FACTORY,
        myExtensions,
        scalarFunctionConverter,
        aggregateFunctionConverter,
        windowFunctionConverter,
        typeConverter);
```

`ConverterProvider` exposes overridable methods for each configurable concern,
including:

| Method | Controls |
| --- | --- |
| `getSqlParserConfig()` | Calcite SQL parsing (identifier casing, DDL parser, conformance) |
| `getCalciteConnectionConfig()` | table-name case sensitivity |
| `getSqlToRelConverterConfig()` | field trimming, sub-query expansion |
| `getSqlOperatorTable()` | which SQL operators/functions are valid |
| `getCallConverters()` | how Calcite `RexCall`s map to Substrait |
| `getScalarFunctionConverter()` / `getAggregateFunctionConverter()` / `getWindowFunctionConverter()` | function conversion |
| `getTypeConverter()` / `getTypeFactory()` / `getTypeSystem()` | type conversion |
| `getSchemaResolver()` | Substrait-to-Calcite schema inference |
| `getExecutionBehavior()` | the plan's execution behavior |

The deepest customization is achieved by extending the class — which is exactly what
the two dynamic providers below do.

## Custom SQL parser configuration

By default Isthmus upper-cases unquoted identifiers (`Casing.TO_UPPER`), uses the DDL
parser factory, and applies lenient SQL conformance. Override `getSqlParserConfig()` to
change this — for example to keep unquoted identifiers in their original case:

```java
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

ConverterProvider provider =
    new ConverterProvider() {
      @Override
      public SqlParser.Config getSqlParserConfig() {
        return SqlParser.Config.DEFAULT
            .withUnquotedCasing(Casing.UNCHANGED)
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withConformance(SqlConformanceEnum.LENIENT);
      }
    };

Plan plan = new SqlToSubstrait(provider).convert(sql, catalog);
```

## Custom functions

Isthmus knows how to translate the standard Substrait function set out of the box. To
teach it functions it does not know — custom functions defined in your own extension
YAML — you register additional signatures with the function converters and put the
resulting `ConverterProvider` to work.

Load your extension YAML, merge it with the defaults if needed, build a
`ScalarFunctionConverter` (and/or aggregate/window converters) that carries the extra
signatures, and assemble a `ConverterProvider`:

```java
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import java.util.List;

// 1. Load the custom extension collection from YAML.
SimpleExtension.ExtensionCollection customExtensions =
    SimpleExtension.load(myFunctionsYaml);

// 2. Describe the matching Calcite operator and register its signature.
SqlFunction customScalarFn =
    new SqlFunction(
        "custom_scalar",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARCHAR),
        null,
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

List<FunctionMappings.Sig> additionalScalarSignatures =
    List.of(FunctionMappings.s(customScalarFn));

// 3. Build a converter that carries those signatures.
ScalarFunctionConverter scalarFunctionConverter =
    new ScalarFunctionConverter(
        customExtensions.scalarFunctions(),
        additionalScalarSignatures,
        SubstraitTypeSystem.TYPE_FACTORY,
        TypeConverter.DEFAULT);

// 4. Assemble a ConverterProvider (aggregate/window converters analogous).
```

The same pattern applies to aggregate and window functions via
`AggregateFunctionConverter` and `WindowFunctionConverter`. `FunctionMappings.s(op)`
creates a signature entry linking a Calcite `SqlOperator` to a Substrait function name.

### User-defined types

Custom functions often involve custom types. Supply a `UserTypeMapper` to a
`TypeConverter` so that Isthmus can translate them in both directions:

```java
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;

TypeConverter typeConverter = new TypeConverter(myUserTypeMapper);
```

`myUserTypeMapper` implements `toSubstrait(RelDataType)` and
`toCalcite(Type.UserDefined)`, returning `null` for types it does not handle. See
[Types & type system](types.md).

## Dynamic providers for UDFs

Registering a Calcite `SqlOperator` by hand for every function is tedious. Two provider
subclasses generate that wiring automatically from your extension collection.

### `DynamicConverterProvider`

`DynamicConverterProvider` treats any scalar function in the extension collection that
is **not** part of the known standard function mappings as a dynamic UDF. It generates
the SQL operators and call converters for those functions automatically, so SQL can
call them without manual configuration:

```java
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.DynamicConverterProvider;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import java.util.List;
import org.apache.calcite.prepare.Prepare;

// Merge custom UDFs with the default catalog.
SimpleExtension.ExtensionCollection extensions =
    DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(
        SimpleExtension.load(List.of("/extensions/scalar_functions_custom.yaml")));

SqlToSubstrait converter = new SqlToSubstrait(new DynamicConverterProvider(extensions));

Prepare.CatalogReader catalog =
    SubstraitCreateStatementParser.processCreateStatementsToCatalog(
        "CREATE TABLE t(x VARCHAR NOT NULL)");

// regexp_extract_custom, format_text, ... are UDFs from the YAML above.
Plan plan = converter.convert("SELECT regexp_extract_custom(x, 'ab') FROM t", catalog);
```

### `AutomaticDynamicFunctionMappingConverterProvider`

`AutomaticDynamicFunctionMappingConverterProvider` goes a step further: it inspects the
scalar, aggregate, **and** window functions of the extension collection, finds those
without an explicit mapping, and auto-generates operators and signatures for all three
kinds. This lets a standard extension function that simply has not been hand-mapped
(for example `strftime` from `functions_datetime.yaml`) be used from SQL without adding
it to the built-in mappings.

```java
import io.substrait.isthmus.AutomaticDynamicFunctionMappingConverterProvider;
import io.substrait.isthmus.SqlToSubstrait;

SqlToSubstrait converter =
    new SqlToSubstrait(new AutomaticDynamicFunctionMappingConverterProvider());
```

## Custom SQL dialects for output

When rendering Substrait back to SQL, the Calcite `SqlDialect` you pass to
[`SubstraitToSql.convert`](substrait-to-sql.md) controls the generated text. Provide a
custom `SqlDialect` (for example subclassing `SparkSqlDialect` and overriding
`unparseCall`) to control how specific operators are rendered for a target engine.

## Related

- [Types & type system](types.md) — `TypeConverter`, `UserTypeMapper`, and the type
  system.
- [core extensions](../core/extensions.md) — loading and merging extension YAMLs.
- [SQL to Substrait](sql-to-substrait.md) — where a customized provider is put to work.
