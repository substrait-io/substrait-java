package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Any;
import io.substrait.expression.Expression.UserDefinedLiteral;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Verify that custom functions can convert from Substrait to Calcite and back. */
class CustomFunctionTest extends PlanTestBase {

  // Define custom functions in a "functions_custom.yaml" extension
  static final String URN = "extension:substrait:functions_custom";
  static final String FUNCTIONS_CUSTOM;

  static {
    try {
      FUNCTIONS_CUSTOM = asString("extensions/functions_custom.yaml");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Load custom extension into an ExtensionCollection
  static final SimpleExtension.ExtensionCollection CUSTOM_EXTENSIONS =
      SimpleExtension.load(FUNCTIONS_CUSTOM);

  // Create user-defined types
  static final String aTypeName = "a_type";
  static final String bTypeName = "b_type";
  static final UserTypeFactory aTypeFactory = new UserTypeFactory(URN, aTypeName);
  static final UserTypeFactory bTypeFactory = new UserTypeFactory(URN, bTypeName);

  // Mapper for user-defined types
  static final UserTypeMapper userTypeMapper =
      new UserTypeMapper() {
        @Nullable
        @Override
        public Type toSubstrait(RelDataType relDataType) {
          if (aTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(URN, aTypeName);
          }
          if (bTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(URN, bTypeName);
          }
          return null;
        }

        @Nullable
        @Override
        public RelDataType toCalcite(Type.UserDefined type) {
          if (type.urn().equals(URN)) {
            if (type.name().equals(aTypeName)) {
              return aTypeFactory.createCalcite(type.nullable());
            }
            if (type.name().equals(bTypeName)) {
              return bTypeFactory.createCalcite(type.nullable());
            }
          }
          return null;
        }
      };

  static final RelDataType varcharType =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createSqlType(SqlTypeName.VARCHAR);
  static final RelDataType varcharArrayType =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createArrayType(varcharType, -1);

  // Define additional signatures for the custom scalar functions
  static final SqlFunction customScalarFn =
      new SqlFunction(
          "custom_scalar",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarAnyFn =
      new SqlFunction(
          "custom_scalar_any",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarAnyToAnyFn =
      new SqlFunction(
          "custom_scalar_any_to_any",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
  static final SqlFunction customScalarAny1Any1ToAny1Fn =
      new SqlFunction(
          "custom_scalar_any1any1_to_any1",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
  static final SqlFunction customScalarAny1Any2ToAny2Fn =
      new SqlFunction(
          "custom_scalar_any1any2_to_any2",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1_NULLABLE,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListAnyFn =
      new SqlFunction(
          "custom_scalar_listany_to_listany",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListAnyAndAnyFn =
      new SqlFunction(
          "custom_scalar_listany_any_to_listany",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListStringFn =
      new SqlFunction(
          "custom_scalar_liststring_to_liststring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(varcharArrayType),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListStringAndAnyFn =
      new SqlFunction(
          "custom_scalar_liststring_any_to_liststring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(varcharArrayType),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListStringAndAnyVariadic0Fn =
      new SqlFunction(
          "custom_scalar_liststring_anyvariadic0_to_liststring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(varcharArrayType),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction customScalarListStringAndAnyVariadic1Fn =
      new SqlFunction(
          "custom_scalar_liststring_anyvariadic1_to_liststring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(varcharArrayType),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction toBType =
      new SqlFunction(
          "to_b_type",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(bTypeFactory.createCalcite(false)),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final List<FunctionMappings.Sig> additionalScalarSignatures =
      List.of(
          FunctionMappings.s(customScalarFn),
          FunctionMappings.s(customScalarAnyFn),
          FunctionMappings.s(customScalarAnyToAnyFn),
          FunctionMappings.s(customScalarAny1Any1ToAny1Fn),
          FunctionMappings.s(customScalarAny1Any2ToAny2Fn),
          FunctionMappings.s(customScalarListAnyFn),
          FunctionMappings.s(customScalarListAnyAndAnyFn),
          FunctionMappings.s(customScalarListStringFn),
          FunctionMappings.s(customScalarListStringAndAnyFn),
          FunctionMappings.s(customScalarListStringAndAnyVariadic0Fn),
          FunctionMappings.s(customScalarListStringAndAnyVariadic1Fn),
          FunctionMappings.s(toBType));

  // Define additional signatures for the custom aggregate functions

  static final SqlAggFunction customAggregateFn =
      new SqlAggFunction(
          "custom_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final SqlAggFunction customTypedAggregateFn =
      new SqlAggFunction(
          "custom_typed_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final SqlAggFunction customEnumAggregateFn =
      new SqlAggFunction(
          "custom_enum_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final SqlAggFunction customFlagsAggregateFn =
      new SqlAggFunction(
          "custom_flags_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final SqlAggFunction customOverlapFn =
      new SqlAggFunction(
          "custom_overlap",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final SqlAggFunction customMixFn =
      new SqlAggFunction(
          "custom_mix",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  static final List<FunctionMappings.Sig> additionalAggregateSignatures =
      List.of(
          FunctionMappings.s(customAggregateFn),
          FunctionMappings.s(customTypedAggregateFn),
          FunctionMappings.s(customEnumAggregateFn),
          FunctionMappings.s(customFlagsAggregateFn),
          FunctionMappings.s(customOverlapFn),
          FunctionMappings.s(customMixFn));

  static TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  // Create Function Converters that can handle the custom functions
  static ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          CUSTOM_EXTENSIONS.scalarFunctions(),
          additionalScalarSignatures,
          SubstraitTypeSystem.TYPE_FACTORY,
          typeConverter);
  static AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          CUSTOM_EXTENSIONS.aggregateFunctions(),
          additionalAggregateSignatures,
          SubstraitTypeSystem.TYPE_FACTORY,
          typeConverter);
  static WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(
          CUSTOM_EXTENSIONS.windowFunctions(), SubstraitTypeSystem.TYPE_FACTORY);

  // Create a SubstraitRelVisitor that uses the custom Function Converters
  final SubstraitRelVisitor calciteToSubstrait = new SubstraitRelVisitor(converterProvider);
  final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(converterProvider);

  CustomFunctionTest() {
    super(
        ConverterProvider.builder()
            .typeFactory(SubstraitTypeSystem.TYPE_FACTORY)
            .extensions(CUSTOM_EXTENSIONS)
            .scalarFunctionConverter(scalarFunctionConverter)
            .aggregateFunctionConverter(aggregateFunctionConverter)
            .windowFunctionConverter(windowFunctionConverter)
            .typeConverter(typeConverter)
            .build());
  }

  @Test
  void customScalarFunctionRoundtrip() {
    // CREATE TABLE example(a TEXT)
    // SELECT custom_scalar(a) FROM example
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(URN, "custom_scalar:str", R.STRING, sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAnyFunctionRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN, "custom_scalar_any:any", R.STRING, sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAnyToAnyFunctionRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN, "custom_scalar_any_to_any:any", R.FP64, sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.FP64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAny1Any1ToAny1FunctionRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_any1any1_to_any1:any_any",
                        R.FP64,
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.FP64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAny1Any1ToAny1FunctionMismatch() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_any1any1_to_any1:any_any",
                        R.FP64,
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.STRING)));

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          RelNode calciteRel = substraitToCalcite.convert(rel);
          calciteToSubstrait.apply(calciteRel);
        },
        "Unable to convert call custom_scalar_any1any1_to_any1(fp64, string)");
  }

  @Test
  void customScalarAny1Any2ToAny2FunctionRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_any1any2_to_any2:any_any",
                        R.STRING,
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListAnyRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_listany_to_listany:list",
                        R.list(R.I64),
                        sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.I64))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListAnyAndAnyRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_listany_any_to_listany:list_any",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(
                List.of("example"), List.of("a", "b"), List.of(R.list(R.STRING), R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_liststring_to_liststring:list",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.STRING))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_liststring_any_to_liststring:list_any",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(
                List.of("example"), List.of("a", "b"), List.of(R.list(R.STRING), R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyVariadic0Roundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic0_to_liststring:list_any",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1),
                        sb.fieldReference(input, 2),
                        sb.fieldReference(input, 3))),
            sb.remap(4),
            sb.namedScan(
                List.of("example"),
                List.of("a", "b", "c", "d"),
                List.of(R.list(R.STRING), R.STRING, R.STRING, R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyVariadic0NoArgsRoundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic0_to_liststring:list_any",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.STRING))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyVariadic1Roundtrip() {
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic1_to_liststring:list_any",
                        R.list(R.STRING),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            sb.namedScan(
                List.of("example"), List.of("a", "b"), List.of(R.list(R.STRING), R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customAggregateFunctionRoundtrip() {
    // CREATE TABLE example (a BIGINT)
    // SELECT custom_aggregate(a) FROM example GROUP BY a
    Rel rel =
        sb.aggregate(
            input -> sb.grouping(input, 0),
            input ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(
                            URN, "custom_aggregate:i64", R.I64, sb.fieldReference(input, 0)))),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void typeArgumentAggregateRoundtrip() {
    // custom_typed_aggregate(<type: i32>, x: i64): only value arguments become Calcite operands,
    // so the type argument can come back only from the carried binding — the wrapper is mandatory
    // for such a declaration, and the reverse conversion restores the type argument without
    // consuming an operand.
    Rel input = sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64));
    Rel rel =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    io.substrait.relation.Aggregate.Measure.builder()
                        .function(
                            io.substrait.expression.AggregateFunctionInvocation.builder()
                                .declaration(
                                    CUSTOM_EXTENSIONS.getAggregateFunction(
                                        SimpleExtension.FunctionAnchor.of(
                                            URN, "custom_typed_aggregate:type_i64")))
                                .outputType(R.I64)
                                .aggregationPhase(
                                    io.substrait.expression.Expression.AggregationPhase
                                        .INITIAL_TO_RESULT)
                                .invocation(
                                    io.substrait.expression.Expression.AggregationInvocation.ALL)
                                .addArguments(TypeCreator.REQUIRED.I32)
                                .addArguments(sb.fieldReference(i, 0))
                                .build())
                        .build()),
            input);

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void enumArgumentAggregateRoundtrip() {
    // custom_enum_aggregate(EXACT|APPROXIMATE, x: i64): no Calcite operator kind encodes this
    // enum (only the std_dev/variance distribution is operator-encoded), so it can come back only
    // from the carried binding — the wrapper is mandatory even though the output type matches
    // Calcite's inference.
    Rel input = sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64));
    Rel rel =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    io.substrait.relation.Aggregate.Measure.builder()
                        .function(
                            io.substrait.expression.AggregateFunctionInvocation.builder()
                                .declaration(
                                    CUSTOM_EXTENSIONS.getAggregateFunction(
                                        SimpleExtension.FunctionAnchor.of(
                                            URN, "custom_enum_aggregate:req_i64")))
                                .outputType(R.I64)
                                .aggregationPhase(
                                    io.substrait.expression.Expression.AggregationPhase
                                        .INITIAL_TO_RESULT)
                                .invocation(
                                    io.substrait.expression.Expression.AggregationInvocation.ALL)
                                .addArguments(io.substrait.expression.EnumArg.of("EXACT"))
                                .addArguments(sb.fieldReference(i, 0))
                                .build())
                        .build()),
            input);

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void argumentTypeMismatchedAggregateKeepsItsBinding() {
    // Under the default FunctionBindingValidation.NONE a plan may invoke custom_aggregate(i64)
    // with an i32 argument. The permissive Calcite operator accepts it and its explicit BIGINT
    // return matches the declared i64, but re-matching would reject the i32 against the
    // declaration's i64 — so the binding must travel for the round trip to reconstruct the
    // invocation as the plan spelled it.
    Rel input = sb.namedScan(List.of("example"), List.of("a"), List.of(R.I32));
    Rel rel =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(
                            URN, "custom_aggregate:i64", R.I64, sb.fieldReference(i, 0)))),
            input);

    RelNode calciteRel = substraitToCalcite.convert(rel);

    org.apache.calcite.rel.core.Aggregate calciteAgg =
        (org.apache.calcite.rel.core.Aggregate) calciteRel;
    assertTrue(
        AggregateFunctions.boundBinding(calciteAgg.getAggCallList().get(0).getAggregation())
            .isPresent());
    assertEquals(rel, calciteToSubstrait.apply(calciteRel));
  }

  @Test
  void wildcardShadowedAggregateKeepsItsBinding() {
    // custom_overlap declares both (i32) and (any, any). The reverse signature matcher checks no
    // per-variant arity — a shorter declaration matches by repeating its trailing argument — so
    // invoking the wildcard impl as f(i32, i32) would re-match the concrete (i32) impl first. The
    // binding must travel for the plan's declaration to survive.
    Rel input = sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.I32, R.I32));
    Rel wildcard =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(
                            URN,
                            "custom_overlap:any_any",
                            R.I64,
                            sb.fieldReference(i, 0),
                            sb.fieldReference(i, 1)))),
            input);

    RelNode wildcardRel = substraitToCalcite.convert(wildcard);
    org.apache.calcite.rel.core.Aggregate wildcardAgg =
        (org.apache.calcite.rel.core.Aggregate) wildcardRel;
    assertTrue(
        AggregateFunctions.boundBinding(wildcardAgg.getAggCallList().get(0).getAggregation())
            .isPresent());
    assertEquals(wildcard, calciteToSubstrait.apply(wildcardRel));

    // The concrete impl itself is reconstructable by its direct key: no wrapper needed.
    Rel concrete =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(URN, "custom_overlap:i32", R.I64, sb.fieldReference(i, 1)))),
            input);
    RelNode concreteRel = substraitToCalcite.convert(concrete);
    org.apache.calcite.rel.core.Aggregate concreteAgg =
        (org.apache.calcite.rel.core.Aggregate) concreteRel;
    assertTrue(
        AggregateFunctions.boundBinding(concreteAgg.getAggCallList().get(0).getAggregation())
            .isEmpty());
    assertEquals(concrete, calciteToSubstrait.apply(concreteRel));
  }

  @Test
  void divergentReturnTypeAggregateKeepsItsBinding() {
    // custom_mix(any, string) declares an i32 return, while the plan declares i64 — permitted
    // under FunctionBindingValidation.NONE and equal to the operator's inference, so nothing about
    // the type forces a wrapper. The reverse signature matcher would reject the declaration by its
    // return type and the singular fallback finds no least-restrictive type for (i64, string), so
    // without the binding the invocation could not be reconstructed at all.
    Rel input = sb.namedScan(List.of("example"), List.of("a", "b"), List.of(R.I64, R.STRING));
    Rel rel =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(
                            URN,
                            "custom_mix:any_str",
                            R.I64,
                            sb.fieldReference(i, 0),
                            sb.fieldReference(i, 1)))),
            input);

    RelNode calciteRel = substraitToCalcite.convert(rel);
    org.apache.calcite.rel.core.Aggregate calciteAgg =
        (org.apache.calcite.rel.core.Aggregate) calciteRel;
    assertTrue(
        AggregateFunctions.boundBinding(calciteAgg.getAggCallList().get(0).getAggregation())
            .isPresent());
    assertEquals(rel, calciteToSubstrait.apply(calciteRel));
  }

  @Test
  void variadicEnumAggregateRoundtrip() {
    // custom_flags_aggregate(x: i64, flag: [A,B,C]...): the trailing enum repeats, and none of the
    // repetitions is a Calcite operand — all of them can come back only from the carried binding.
    Rel input = sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64));
    Rel rel =
        sb.aggregate(
            i -> sb.grouping(i, 0),
            i ->
                List.of(
                    io.substrait.relation.Aggregate.Measure.builder()
                        .function(
                            io.substrait.expression.AggregateFunctionInvocation.builder()
                                .declaration(
                                    CUSTOM_EXTENSIONS.getAggregateFunction(
                                        SimpleExtension.FunctionAnchor.of(
                                            URN, "custom_flags_aggregate:i64_req")))
                                .outputType(R.I64)
                                .aggregationPhase(
                                    io.substrait.expression.Expression.AggregationPhase
                                        .INITIAL_TO_RESULT)
                                .invocation(
                                    io.substrait.expression.Expression.AggregationInvocation.ALL)
                                .addArguments(sb.fieldReference(i, 0))
                                .addArguments(io.substrait.expression.EnumArg.of("A"))
                                .addArguments(io.substrait.expression.EnumArg.of("B"))
                                .build())
                        .build()),
            input);

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void aggregateInferenceFailureFallsBackToThePlanTypeUnderPlanOutput() {
    // custom_aggregate mapped to an operator whose return-type inference rejects its operands.
    // Under PLAN_OUTPUT the inferred type only decides whether the plan's type must travel on a
    // wrapper, so the failure counts as "diverges" and the conversion proceeds with the plan's
    // type; under CALCITE_INFERENCE there is nothing to fall back to and the failure propagates.
    SqlAggFunction throwingAggregateFn =
        new SqlAggFunction(
            "custom_aggregate",
            SqlKind.OTHER_FUNCTION,
            opBinding -> {
              throw new IllegalStateException("this operator cannot infer a return type");
            },
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION) {};
    AggregateFunctionConverter throwingAggregateConverter =
        new AggregateFunctionConverter(
            CUSTOM_EXTENSIONS.aggregateFunctions(),
            List.of(FunctionMappings.s(throwingAggregateFn)),
            SubstraitTypeSystem.TYPE_FACTORY,
            typeConverter);
    Rel rel =
        sb.aggregate(
            input -> sb.grouping(input, 0),
            input ->
                List.of(
                    sb.measure(
                        sb.aggregateFn(
                            URN, "custom_aggregate:i64", R.I64, sb.fieldReference(input, 0)))),
            sb.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    ConverterProvider planOutput =
        ConverterProvider.builder()
            .typeFactory(SubstraitTypeSystem.TYPE_FACTORY)
            .extensions(CUSTOM_EXTENSIONS)
            .aggregateFunctionConverter(throwingAggregateConverter)
            .typeConverter(typeConverter)
            .build();
    org.apache.calcite.rel.core.Aggregate calciteAgg =
        (org.apache.calcite.rel.core.Aggregate) new SubstraitToCalcite(planOutput).convert(rel);
    assertEquals(SqlTypeName.BIGINT, calciteAgg.getAggCallList().get(0).getType().getSqlTypeName());

    ConverterProvider calciteInference =
        ConverterProvider.builder()
            .typeFactory(SubstraitTypeSystem.TYPE_FACTORY)
            .extensions(CUSTOM_EXTENSIONS)
            .aggregateFunctionConverter(throwingAggregateConverter)
            .typeConverter(typeConverter)
            .aggregateConversion(
                new AggregateConversion(
                    AggregateConversion.OutputTypeSource.CALCITE_INFERENCE,
                    AggregateConversion.FunctionBindingValidation.NONE))
            .build();
    assertThrows(
        IllegalStateException.class, () -> new SubstraitToCalcite(calciteInference).convert(rel));
  }

  @Test
  void customTypesInFunctionsRoundtrip() {
    // CREATE TABLE example(a a_type)
    // SELECT to_b_type(a) FROM example
    Rel rel =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        "to_b_type:u!a_type",
                        R.userDefined(URN, "b_type"),
                        sb.fieldReference(input, 0))),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customTypesLiteralInFunctionsRoundtrip() {
    Builder bldr = Expression.Literal.newBuilder();
    Any anyValue = Any.pack(bldr.setI32(10).build());
    UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue);

    Rel rel1 =
        sb.project(
            input ->
                List.of(sb.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), val)),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    RelNode calciteRel = substraitToCalcite.convert(rel1);
    Rel rel2 = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel1, rel2);

    ExtensionCollector extensionCollector = new ExtensionCollector();
    io.substrait.proto.Rel protoRel = new RelProtoConverter(extensionCollector).toProto(rel1);
    Rel rel3 = new ProtoRelConverter(extensionCollector, CUSTOM_EXTENSIONS).from(protoRel);
    assertEquals(rel1, rel3);
  }

  @Test
  void customNullableUserDefinedLiteralRoundtrip() {
    Builder bldr = Expression.Literal.newBuilder();
    Any anyValue = Any.pack(bldr.setI32(10).build());
    UserDefinedLiteral nullableLiteral =
        ExpressionCreator.userDefinedLiteralAny(
            true, URN, "a_type", java.util.Collections.emptyList(), anyValue);

    Rel rel =
        sb.project(
            input -> List.of(nullableLiteral),
            sb.remap(1),
            sb.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));
    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }
}
