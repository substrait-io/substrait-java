package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Any;
import io.substrait.dsl.SubstraitBuilder;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Verify that custom functions can convert from Substrait to Calcite and back. */
public class CustomFunctionTest extends PlanTestBase {

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
  static final SimpleExtension.ExtensionCollection extensionCollection =
      SimpleExtension.load("custom.yaml", FUNCTIONS_CUSTOM);

  final SubstraitBuilder b = new SubstraitBuilder(extensionCollection);

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

  // Define additional mapping signatures for the custom scalar functions
  final List<FunctionMappings.Sig> additionalScalarSignatures =
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

  // Define additional mapping signatures for the custom aggregate functions
  final List<FunctionMappings.Sig> additionalAggregateSignatures =
      List.of(FunctionMappings.s(customAggregateFn));

  static final SqlAggFunction customAggregateFn =
      new SqlAggFunction(
          "custom_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  // Create Function Converters that can handle the custom functions
  ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          extensionCollection.scalarFunctions(),
          additionalScalarSignatures,
          typeFactory,
          typeConverter);
  AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          extensionCollection.aggregateFunctions(),
          additionalAggregateSignatures,
          typeFactory,
          typeConverter);
  WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(extensionCollection.windowFunctions(), typeFactory);

  final SubstraitToCalcite substraitToCalcite =
      new CustomSubstraitToCalcite(extensionCollection, typeFactory, typeConverter);

  // Create a SubstraitRelVisitor that uses the custom Function Converters
  final SubstraitRelVisitor calciteToSubstrait =
      new SubstraitRelVisitor(
          typeFactory,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          typeConverter,
          ImmutableFeatureBoard.builder().build());

  // Create a SubstraitToCalcite converter that has access to the custom Function Converters
  class CustomSubstraitToCalcite extends SubstraitToCalcite {

    public CustomSubstraitToCalcite(
        SimpleExtension.ExtensionCollection extensions,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      super(extensions, typeFactory, typeConverter);
    }

    @Override
    protected SubstraitRelNodeConverter createSubstraitRelNodeConverter(RelBuilder relBuilder) {
      return new SubstraitRelNodeConverter(
          typeFactory,
          relBuilder,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          typeConverter);
    }
  }

  // Helper methods for roundtrip assertions

  /** Assert that a relation roundtrips correctly through Calcite conversion. */
  private void assertCalciteRoundtrip(Rel originalRel) {
    RelNode calciteRel = substraitToCalcite.convert(originalRel);
    Rel calciteRoundtrippedRel = calciteToSubstrait.apply(calciteRel);
    assertEquals(originalRel, calciteRoundtrippedRel);
  }

  /** Assert that a relation roundtrips correctly through Proto serialization. */
  private void assertProtoRoundtrip(Rel originalRel) {
    ExtensionCollector extensionCollector = new ExtensionCollector();
    io.substrait.proto.Rel protoRel =
        new RelProtoConverter(extensionCollector).toProto(originalRel);
    Rel protoRoundtrippedRel =
        new ProtoRelConverter(extensionCollector, extensionCollection).from(protoRel);
    assertEquals(originalRel, protoRoundtrippedRel);
  }

  /** Assert that a relation roundtrips correctly through both Calcite and Proto conversions. */
  private void assertRoundtrip(Rel originalRel) {
    assertCalciteRoundtrip(originalRel);
    assertProtoRoundtrip(originalRel);
  }

  @Test
  void customScalarFunctionRoundtrip() {
    // CREATE TABLE example(a TEXT)
    // SELECT custom_scalar(a) FROM example
    Rel rel =
        b.project(
            input ->
                List.of(b.scalarFn(URN, "custom_scalar:str", R.STRING, b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAnyFunctionRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(URN, "custom_scalar_any:any", R.STRING, b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAnyToAnyFunctionRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN, "custom_scalar_any_to_any:any", R.FP64, b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.FP64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAny1Any1ToAny1FunctionRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_any1any1_to_any1:any_any",
                        R.FP64,
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.FP64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarAny1Any1ToAny1FunctionMismatch() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_any1any1_to_any1:any_any",
                        R.FP64,
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.STRING)));

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
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_any1any2_to_any2:any_any",
                        R.STRING,
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.FP64, R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListAnyRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_listany_to_listany:list",
                        R.list(R.I64),
                        b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.I64))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListAnyAndAnyRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_listany_any_to_listany:list_any",
                        R.list(R.STRING),
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(
                List.of("example"), List.of("a", "b"), List.of(R.list(R.STRING), R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_liststring_to_liststring:list",
                        R.list(R.STRING),
                        b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.STRING))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyRoundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_liststring_any_to_liststring:list_any",
                        R.list(R.STRING),
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(
                List.of("example"), List.of("a", "b"), List.of(R.list(R.STRING), R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyVariadic0Roundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic0_to_liststring:list_any",
                        R.list(R.STRING),
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1),
                        b.fieldReference(input, 2),
                        b.fieldReference(input, 3))),
            b.remap(4),
            b.namedScan(
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
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic0_to_liststring:list_any",
                        R.list(R.STRING),
                        b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.list(R.STRING))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customScalarListStringAndAnyVariadic1Roundtrip() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "custom_scalar_liststring_anyvariadic1_to_liststring:list_any",
                        R.list(R.STRING),
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 1))),
            b.remap(2),
            b.namedScan(
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
        b.aggregate(
            input -> b.grouping(input, 0),
            input ->
                List.of(
                    b.measure(
                        b.aggregateFn(
                            URN, "custom_aggregate:i64", R.I64, b.fieldReference(input, 0)))),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customTypesInFunctionsRoundtrip() {
    // CREATE TABLE example(a a_type)
    // SELECT to_b_type(a) FROM example
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        URN,
                        "to_b_type:u!a_type",
                        R.userDefined(URN, "b_type"),
                        b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customTypesLiteralInFunctionsRoundtrip() {
    Expression.Literal.Builder bldr = Expression.Literal.newBuilder();
    Any anyValue = Any.pack(bldr.setI32(10).build());
    UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "a_type", anyValue);

    Rel originalRel =
        b.project(
            input ->
                List.of(b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), val)),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertRoundtrip(originalRel);
  }

  @Test
  void multipleDifferentUserDefinedAnyTypesProtoRoundtrip() {
    // Test that UserDefinedAny literals with different payload types have different type names
    // a_type wraps int, b_type wraps string - proto only
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI32(100).build());
    UserDefinedLiteral aTypeLit =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "a_type", anyValue1);

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Any anyValue2 = Any.pack(bldr2.setString("b_value").build());
    UserDefinedLiteral bTypeLit =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "b_type", anyValue2);

    Rel originalRel =
        b.project(
            input -> List.of(aTypeLit, bTypeLit),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }

  @Test
  void userDefinedStructWithPrimitivesProtoRoundtrip() {
    // Test UserDefinedStruct with various primitive field types - proto roundtrip only
    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.string(false, "hello"))
            .addFields(ExpressionCreator.bool(false, true))
            .addFields(ExpressionCreator.fp64(false, 2.718))
            .build();

    Rel originalRel =
        b.project(
            input ->
                List.of(b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), val)),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }

  @Test
  void userDefinedStructWithNestedStructProtoRoundtrip() {
    // Test UserDefinedStruct with nested struct fields - proto roundtrip only
    io.substrait.expression.Expression.StructLiteral innerStruct =
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(false, 10), ExpressionCreator.string(false, "nested"));

    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 100))
            .addFields(innerStruct)
            .addFields(ExpressionCreator.bool(false, false))
            .build();

    Rel originalRel =
        b.project(
            input ->
                List.of(b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), val)),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }

  @Test
  void multipleUserDefinedStructDifferentStructuresProtoRoundtrip() {
    // Test multiple UserDefinedStruct types with different struct schemas
    // a_type: {content: string}
    // b_type: {content_int: i32, content_fp: fp64}
    io.substrait.expression.Expression.UserDefinedStruct aTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.string(false, "hello"))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct bTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("b_type")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.fp64(false, 3.14159))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(aTypeStruct, bTypeStruct),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }

  @Test
  void intermixedUserDefinedAnyAndStructProtoRoundtrip() {
    // Test intermixing UserDefinedAny and UserDefinedStruct in the same query
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI64(999L).build());
    UserDefinedLiteral anyLit1 =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "a_type", anyValue1);

    io.substrait.expression.Expression.UserDefinedStruct structLit1 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 123))
            .addFields(ExpressionCreator.bool(false, false))
            .build();

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Any anyValue2 = Any.pack(bldr2.setString("mixed").build());
    UserDefinedLiteral anyLit2 =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "a_type", anyValue2);

    io.substrait.expression.Expression.UserDefinedStruct structLit2 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.fp64(false, 1.414))
            .build();

    Rel originalRel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), anyLit1),
                    b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), structLit1),
                    b.scalarFn(URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), anyLit2),
                    b.scalarFn(
                        URN, "to_b_type:u!a_type", R.userDefined(URN, "b_type"), structLit2)),
            b.remap(4),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }

  @Test
  void multipleDifferentUDTTypesWithAnyAndStructProtoRoundtrip() {
    // Test multiple different UDT type names (a_type, b_type) with both Any and Struct
    Expression.Literal.Builder aTypeBldr = Expression.Literal.newBuilder();
    Any aTypeAny = Any.pack(aTypeBldr.setI32(42).build());
    UserDefinedLiteral aTypeAny1 =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "a_type", aTypeAny);

    io.substrait.expression.Expression.UserDefinedStruct aTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 100))
            .build();

    Expression.Literal.Builder bTypeBldr = Expression.Literal.newBuilder();
    Any bTypeAny = Any.pack(bTypeBldr.setString("b_val").build());
    UserDefinedLiteral bTypeAny1 =
        ExpressionCreator.userDefinedLiteralAny(false, URN, "b_type", bTypeAny);

    io.substrait.expression.Expression.UserDefinedStruct bTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("b_type")
            .addFields(ExpressionCreator.string(false, "struct_b"))
            .addFields(ExpressionCreator.bool(false, true))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(aTypeAny1, aTypeStruct, bTypeAny1, bTypeStruct),
            b.remap(4),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertProtoRoundtrip(originalRel);
  }
}
