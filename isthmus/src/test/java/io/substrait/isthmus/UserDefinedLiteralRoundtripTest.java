package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class UserDefinedLiteralRoundtripTest extends PlanTestBase {

  private static final String NESTED_TYPES_URN = "extension:io.substrait:test_nested_types";

  private static final String NESTED_TYPES_YAML =
      "---\n"
          + "urn: "
          + NESTED_TYPES_URN
          + "\n"
          + "types:\n"
          + "  - name: point\n"
          + "    structure:\n"
          + "      latitude: i32\n"
          + "      longitude: i32\n"
          + "  - name: triangle\n"
          + "    structure:\n"
          + "      p1: point\n"
          + "      p2: point\n"
          + "      p3: point\n"
          + "  - name: vector\n"
          + "    parameters:\n"
          + "      - name: T\n"
          + "        type: dataType\n"
          + "    structure:\n"
          + "      x: T\n"
          + "      y: T\n"
          + "      z: T\n"
          + "  - name: multi_param\n"
          + "    parameters:\n"
          + "      - name: T\n"
          + "        type: dataType\n"
          + "      - name: size\n"
          + "        type: integer\n"
          + "      - name: nullable\n"
          + "        type: boolean\n"
          + "      - name: encoding\n"
          + "        type: string\n"
          + "      - name: precision\n"
          + "        type: dataType\n"
          + "      - name: mode\n"
          + "        type: enum\n"
          + "    structure:\n"
          + "      value: T\n";

  private static final SimpleExtension.ExtensionCollection NESTED_TYPES_EXTENSIONS =
      SimpleExtension.load("nested_types.yaml", NESTED_TYPES_YAML);

  private final SubstraitBuilder builder = new SubstraitBuilder(NESTED_TYPES_EXTENSIONS);

  private final Map<String, UserTypeFactory> userTypeFactories =
      Map.of(
          "point", new UserTypeFactory(NESTED_TYPES_URN, "point"),
          "triangle", new UserTypeFactory(NESTED_TYPES_URN, "triangle"),
          "vector", new UserTypeFactory(NESTED_TYPES_URN, "vector"),
          "multi_param", new UserTypeFactory(NESTED_TYPES_URN, "multi_param"));

  private final UserTypeMapper userTypeMapper =
      new UserTypeMapper() {
        @Override
        public @Nullable Type toSubstrait(RelDataType relDataType) {
          return userTypeFactories.values().stream()
              .filter(factory -> factory.isTypeFromFactory(relDataType))
              .findFirst()
              .map(
                  factory ->
                      factory.createSubstrait(
                          relDataType.isNullable(), factory.getTypeParameters(relDataType)))
              .orElse(null);
        }

        @Override
        public @Nullable RelDataType toCalcite(Type.UserDefined type) {
          if (!type.urn().equals(NESTED_TYPES_URN)) {
            return null;
          }
          UserTypeFactory factory = userTypeFactories.get(type.name());
          if (factory == null) {
            return null;
          }

          return factory.createCalcite(type.nullable(), type.typeParameters());
        }
      };

  private final TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  private final ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          NESTED_TYPES_EXTENSIONS.scalarFunctions(),
          Collections.emptyList(),
          typeFactory,
          typeConverter);

  private final AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          NESTED_TYPES_EXTENSIONS.aggregateFunctions(),
          Collections.emptyList(),
          typeFactory,
          typeConverter);

  private final WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(NESTED_TYPES_EXTENSIONS.windowFunctions(), typeFactory);

  private final SubstraitToCalcite substraitToCalcite =
      new SubstraitToCalcite(NESTED_TYPES_EXTENSIONS, typeFactory, typeConverter);

  private final SubstraitRelVisitor calciteToSubstrait =
      new SubstraitRelVisitor(
          typeFactory,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          typeConverter,
          ImmutableFeatureBoard.builder().build());

  private void assertRoundTrip(Expression.UserDefinedLiteral literal) {
    Rel rel =
        builder.project(
            input -> List.of(literal),
            builder.remap(1),
            builder.namedScan(List.of("example"), List.of("udt_col"), List.of(literal.getType())));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  private Expression.UserDefinedStructLiteral pointStructLiteral(int latitude, int longitude) {
    return ExpressionCreator.userDefinedLiteralStruct(
        false,
        NESTED_TYPES_URN,
        "point",
        Collections.emptyList(),
        Arrays.asList(
            ExpressionCreator.i32(false, latitude), ExpressionCreator.i32(false, longitude)));
  }

  private Expression.UserDefinedStructLiteral triangleStruct(
      Expression.UserDefinedLiteral p1,
      Expression.UserDefinedLiteral p2,
      Expression.UserDefinedLiteral p3) {
    return ExpressionCreator.userDefinedLiteralStruct(
        false, NESTED_TYPES_URN, "triangle", Collections.emptyList(), Arrays.asList(p1, p2, p3));
  }

  private Expression.UserDefinedAnyLiteral pointAnyLiteral(String value) {
    return ExpressionCreator.userDefinedLiteralAny(
        false, NESTED_TYPES_URN, "point", Collections.emptyList(), Any.pack(StringValue.of(value)));
  }

  private Expression.UserDefinedStructLiteral vectorStructLiteral(
      List<Type.Parameter> params,
      Expression.Literal x,
      Expression.Literal y,
      Expression.Literal z) {
    return ExpressionCreator.userDefinedLiteralStruct(
        false, NESTED_TYPES_URN, "vector", params, Arrays.asList(x, y, z));
  }

  @Test
  void anyEncodedUdtRoundTrip() {
    Expression.UserDefinedLiteral literal =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            Any.pack(StringValue.of("<Some User-Defined Representation>")));

    assertRoundTrip(literal);
  }

  @Test
  void structEncodedUdtRoundTrip() {
    assertRoundTrip(pointStructLiteral(42, 100));
  }

  @Test
  void nestedStructEncodedUdtRoundTrip() {
    assertRoundTrip(
        triangleStruct(
            pointStructLiteral(0, 0), pointStructLiteral(10, 0), pointStructLiteral(5, 10)));
  }

  @Test
  void nestedMixedEncodingsRoundTrip() {
    // Mix encodings: struct, any, struct.
    assertRoundTrip(
        triangleStruct(
            pointStructLiteral(1, 2), pointAnyLiteral("p2-any"), pointStructLiteral(3, 4)));
  }

  @Test
  void parameterizedUdtRoundTrip() {
    Type.Parameter typeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.I32.builder().nullable(false).build())
            .build();

    Expression.UserDefinedLiteral literal =
        vectorStructLiteral(
            Collections.singletonList(typeParam),
            ExpressionCreator.i32(false, 1),
            ExpressionCreator.i32(false, 2),
            ExpressionCreator.i32(false, 3));

    assertRoundTrip(literal);
  }

  @Test
  void parameterizedUdtAllParamKindsRoundTrip() {
    Type.Parameter typeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.I32.builder().nullable(false).build())
            .build();

    Type.Parameter intParam =
        io.substrait.type.ImmutableType.ParameterIntegerValue.builder().value(100L).build();

    Type.Parameter boolParam =
        io.substrait.type.ImmutableType.ParameterBooleanValue.builder().value(true).build();

    Type.Parameter stringParam =
        io.substrait.type.ImmutableType.ParameterStringValue.builder().value("utf8").build();

    Type.Parameter nullParam = io.substrait.type.Type.ParameterNull.INSTANCE;

    Type.Parameter enumParam =
        io.substrait.type.ImmutableType.ParameterEnumValue.builder().value("FAST").build();

    Expression.UserDefinedLiteral literal =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "multi_param",
            Arrays.asList(typeParam, intParam, boolParam, stringParam, nullParam, enumParam),
            Arrays.asList(ExpressionCreator.i32(false, 42)));

    assertRoundTrip(literal);
  }

  @Test
  void nullableFieldsInStructUdtRoundTrip() {
    // Test field-level nullability: struct is non-nullable, but fields are nullable
    Expression.UserDefinedStructLiteral literal =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            Arrays.asList(
                ExpressionCreator.i32(true, 42), // nullable field
                ExpressionCreator.i32(true, 100))); // nullable field

    assertRoundTrip(literal);
  }

  @Test
  void mixedFieldNullabilityInStructUdtRoundTrip() {
    // Test mixed field nullability: struct is non-nullable, first field nullable, second
    // non-nullable
    Expression.UserDefinedStructLiteral literal =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            Arrays.asList(
                ExpressionCreator.i32(true, 42), // nullable field
                ExpressionCreator.i32(false, 100))); // non-nullable field

    assertRoundTrip(literal);
  }

  @Test
  void nullableStructEncodedUdtRoundTrip() {
    // Test struct-level nullability: struct is nullable, fields are non-nullable
    Expression.UserDefinedStructLiteral literal =
        ExpressionCreator.userDefinedLiteralStruct(
            true,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            Arrays.asList(
                ExpressionCreator.i32(false, 42), // non-nullable field
                ExpressionCreator.i32(false, 100))); // non-nullable field

    assertRoundTrip(literal);
  }

  @Test
  void nullableStructWithMixedFieldNullabilityRoundTrip() {
    // Test the critical case: nullable struct with mixed field nullability
    Expression.UserDefinedStructLiteral literal =
        ExpressionCreator.userDefinedLiteralStruct(
            true,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            Arrays.asList(
                ExpressionCreator.i32(true, 42), // nullable field
                ExpressionCreator.i32(false, 100))); // non-nullable field

    assertRoundTrip(literal);
  }

  @Test
  void multipleParameterizedUdtInstancesRoundTrip() {
    Type.Parameter i32Param =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.I32.builder().nullable(false).build())
            .build();
    Type.Parameter fp64Param =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.FP64.builder().nullable(false).build())
            .build();

    Expression.UserDefinedLiteral vecI32 =
        vectorStructLiteral(
            Collections.singletonList(i32Param),
            ExpressionCreator.i32(false, 1),
            ExpressionCreator.i32(false, 2),
            ExpressionCreator.i32(false, 3));

    Expression.UserDefinedLiteral vecFp64 =
        vectorStructLiteral(
            Collections.singletonList(fp64Param),
            ExpressionCreator.fp64(false, 1.1),
            ExpressionCreator.fp64(false, 2.2),
            ExpressionCreator.fp64(false, 3.3));

    Rel rel = builder.project(input -> Arrays.asList(vecI32, vecFp64), builder.emptyScan());

    RelNode calciteRel = substraitToCalcite.convert(rel);
    Rel relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }
}
