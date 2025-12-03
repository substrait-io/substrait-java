package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeExpressionEvaluator;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for conversion of SimpleExtension function definitions to Calcite SqlOperators. */
class SimpleExtensionToSqlOperatorTest {

  private static final String CUSTOM_FUNCTION_PATH = "/extensions/scalar_functions_custom.yaml";

  private static final SimpleExtension.ExtensionCollection EXTENSIONS =
      SimpleExtension.load(
          CUSTOM_FUNCTION_PATH,
          SimpleExtensionToSqlOperatorTest.class.getResourceAsStream(CUSTOM_FUNCTION_PATH));

  private static final List<SqlOperator> OPERATORS = SimpleExtensionToSqlOperator.from(EXTENSIONS);

  /** Data carrier for test cases. */
  record TestSpec(
      String name,
      int minArgs,
      int maxArgs,
      SimpleExtension.Nullability nullability,
      String expectedReturnType,
      Consumer<SqlOperator> customValidator) {

    TestSpec(
        final String name,
        final int min,
        final int max,
        final SimpleExtension.Nullability nullability,
        final String returnType) {
      this(name, min, max, nullability, returnType, op -> {});
    }
  }

  @ParameterizedTest
  @MethodSource("provideTestSpecs")
  void testCustomUdfConversion(final TestSpec spec) {
    final SqlOperator operator = findOperator(spec.name);
    final SimpleExtension.Function funcDef = findFunctionDef(spec.name);

    // 1. Verify Argument Counts
    assertEquals(
        spec.minArgs,
        operator.getOperandCountRange().getMin(),
        () -> spec.name + ": Incorrect min args");
    assertEquals(
        spec.maxArgs,
        operator.getOperandCountRange().getMax(),
        () -> spec.name + ": Incorrect max args");
    assertNotNull(operator.getOperandTypeChecker(), () -> spec.name + ": Type checker missing");

    // 2. Verify Nullability (if specified)
    if (spec.nullability != null) {
      assertEquals(
          spec.nullability, funcDef.nullability(), () -> spec.name + ": Incorrect nullability");
    }

    // 3. Verify Return Type
    verifyReturnType(operator, funcDef, spec.expectedReturnType);

    // 4. Custom Validation
    spec.customValidator.accept(operator);
  }

  private static Stream<TestSpec> provideTestSpecs() {
    return Stream.of(
        new TestSpec(
            "REGEXP_EXTRACT_CUSTOM",
            2,
            2,
            null,
            "VARCHAR",
            op -> {
              final String sigs =
                  op.getOperandTypeChecker().getAllowedSignatures(op, op.getName()).toLowerCase();
              // Calcite represents string families as <character>
              assertTrue(
                  sigs.contains("varchar") || sigs.contains("string") || sigs.contains("character"),
                  () -> "Signatures should contain string types. Actual: " + sigs);
            }),
        new TestSpec("FORMAT_TEXT", 2, 2, SimpleExtension.Nullability.MIRROR, "VARCHAR"),
        new TestSpec(
            "SYSTEM_PROPERTY_GET", 1, 1, SimpleExtension.Nullability.DECLARED_OUTPUT, "VARCHAR"),
        new TestSpec("SAFE_DIVIDE_CUSTOM", 2, 2, SimpleExtension.Nullability.DISCRETE, "REAL"));
  }

  private void verifyReturnType(
      final SqlOperator operator,
      final SimpleExtension.Function funcDef,
      final String expectedTypeName) {
    assertNotNull(funcDef.returnType(), "Return type missing in YAML");
    assertNotNull(operator.getReturnTypeInference(), "SQL Operator missing return type inference");

    // 1. Evaluate expected type from YAML
    final Type expectedType =
        TypeExpressionEvaluator.evaluateExpression(
            funcDef.returnType(), funcDef.args(), Collections.emptyList());

    // 2. Convert expected Substrait type to Calcite type
    final RelDataType expectedCalciteType =
        TypeConverter.DEFAULT.toCalcite(SubstraitTypeSystem.TYPE_FACTORY, expectedType);

    // 3. Validate consistency: Ensure YAML derived type matches the TestSpec expectation string
    // This utilizes the previously unused 'expectedTypeName'
    assertEquals(
        expectedTypeName,
        expectedCalciteType.getSqlTypeName().toString(),
        () ->
            "YAML definition derived type does not match TestSpec expectation for "
                + funcDef.name());

    // 4. Infer actual type from the Calcite Operator using a minimal binding
    final RelDataType actualReturnType =
        operator.getReturnTypeInference().inferReturnType(createMockBinding(operator));

    // 5. Compare Derived Expectation vs Actual Operator Inference
    assertEquals(
        expectedCalciteType.getSqlTypeName(),
        actualReturnType.getSqlTypeName(),
        () -> "Return type mismatch for " + funcDef.name());
    assertEquals(
        expectedCalciteType.isNullable(),
        actualReturnType.isNullable(),
        () -> "Nullability mismatch for " + funcDef.name());
  }

  private SqlOperator findOperator(final String name) {
    return OPERATORS.stream()
        .filter(o -> o.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Operator not found: " + name));
  }

  private SimpleExtension.Function findFunctionDef(final String name) {
    return EXTENSIONS.scalarFunctions().stream()
        .filter(f -> f.name().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("YAML Definition not found: " + name));
  }

  /** Minimal anonymous implementation of SqlOperatorBinding to support return type inference. */
  private SqlOperatorBinding createMockBinding(final SqlOperator operator) {
    final RelDataTypeFactory typeFactory = SubstraitTypeSystem.TYPE_FACTORY;
    return new SqlOperatorBinding(typeFactory, operator) {
      @Override
      public int getOperandCount() {
        return 0;
      }

      @Override
      public RelDataType getOperandType(final int ordinal) {
        throw new IndexOutOfBoundsException();
      }

      @Override
      public CalciteException newError(final Resources.ExInst<SqlValidatorException> e) {
        return new CalciteException(e.toString(), null);
      }
    };
  }
}
