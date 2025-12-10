package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeExpressionEvaluator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for conversion of SimpleExtension function definitions to Calcite SqlOperators. */
class SimpleExtensionToSqlOperatorTest {

  private static final String CUSTOM_FUNCTION_PATH = "/extensions/scalar_functions_custom.yaml";
  private static final RelDataTypeFactory TYPE_FACTORY = SubstraitTypeSystem.TYPE_FACTORY;

  private static final Map<String, SimpleExtension.Function> FUNCTION_DEFS;
  private static final Map<String, SqlOperator> OPERATORS;

  static {
    final SimpleExtension.ExtensionCollection extensions =
        SimpleExtension.load(
            CUSTOM_FUNCTION_PATH,
            SimpleExtensionToSqlOperatorTest.class.getResourceAsStream(CUSTOM_FUNCTION_PATH));

    FUNCTION_DEFS =
        extensions.scalarFunctions().stream()
            .collect(
                Collectors.toUnmodifiableMap(f -> f.name().toLowerCase(), Function.identity()));

    OPERATORS =
        SimpleExtensionToSqlOperator.from(extensions).stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    op -> op.getName().toLowerCase(), Function.identity()));
  }

  /** Test Specification. */
  record TestSpec(
      String name,
      int minArgs,
      int maxArgs,
      SimpleExtension.Nullability nullability,
      List<String> expectedArgTypes) {}

  @ParameterizedTest
  @MethodSource("provideTestSpecs")
  void testCustomUdfConversion(final TestSpec spec) {
    final SqlOperator operator = getOperator(spec.name);
    final SimpleExtension.Function funcDef = getFunctionDef(spec.name);

    assertEquals(
        spec.minArgs,
        operator.getOperandCountRange().getMin(),
        () -> spec.name + ": Incorrect min args");
    assertEquals(
        spec.maxArgs,
        operator.getOperandCountRange().getMax(),
        () -> spec.name + ": Incorrect max args");

    if (spec.nullability != null) {
      assertEquals(
          spec.nullability, funcDef.nullability(), () -> spec.name + ": Incorrect nullability");
    }

    if (!spec.expectedArgTypes.isEmpty()) {
      verifyAllowedSignatures(operator, spec.expectedArgTypes);
    }

    verifyReturnTypeConsistency(operator, funcDef);
  }

  private static Stream<TestSpec> provideTestSpecs() {
    return Stream.of(
        new TestSpec("REGEXP_EXTRACT_CUSTOM", 2, 2, null, List.of("VARCHAR", "VARCHAR")),
        new TestSpec(
            "FORMAT_TEXT", 2, 2, SimpleExtension.Nullability.MIRROR, List.of("VARCHAR", "VARCHAR")),
        new TestSpec(
            "SYSTEM_PROPERTY_GET",
            1,
            1,
            SimpleExtension.Nullability.DECLARED_OUTPUT,
            List.of("VARCHAR")),
        new TestSpec(
            "SAFE_DIVIDE_CUSTOM",
            2,
            2,
            SimpleExtension.Nullability.DISCRETE,
            List.of("INTEGER", "INTEGER")));
  }

  /**
   * Parses the operator's signature string and checks that the types match the expected list
   * index-by-index.
   */
  private void verifyAllowedSignatures(
      final SqlOperator operator, final List<String> expectedArgTypes) {
    assertNotNull(operator.getOperandTypeChecker(), "Operand type checker is null");

    // e.g., "SAFE_DIVIDE_CUSTOM(<NUMERIC>, <NUMERIC>)"
    final String signature =
        operator
            .getOperandTypeChecker()
            .getAllowedSignatures(operator, operator.getName())
            .toUpperCase();

    // Regex to capture arguments inside parentheses: NAME(ARG1, ARG2)
    final Pattern pattern = Pattern.compile(".*?\\((.*)\\).*");
    final Matcher matcher = pattern.matcher(signature);

    assertTrue(matcher.matches(), () -> "Signature format not recognized: " + signature);

    // Split args by comma (assuming simple types for this test suite)
    final String argsPart = matcher.group(1);
    final List<String> actualArgTypes =
        Arrays.stream(argsPart.split(",")).map(String::trim).toList();

    assertEquals(
        expectedArgTypes.size(),
        actualArgTypes.size(),
        () -> "Signature argument count mismatch. Signature: " + signature);

    // Positional Check
    for (int i = 0; i < expectedArgTypes.size(); i++) {
      final String expected = expectedArgTypes.get(i);
      final String actual = actualArgTypes.get(i);

      final SqlTypeName sqlTypeName = SqlTypeName.valueOf(expected);
      final String familyName = sqlTypeName.getFamily().toString();

      // Check if the actual slot matches the specific type OR the generic family
      // e.g. Expected "INTEGER" matches actual "<NUMERIC>" or "INTEGER"
      final boolean match = actual.contains(expected) || actual.contains(familyName);

      final int index = i;
      assertTrue(
          match,
          () ->
              "Argument mismatch at index "
                  + index
                  + ".\n"
                  + "Expected: "
                  + expected
                  + " (Family: "
                  + familyName
                  + ")\n"
                  + "Actual: "
                  + actual
                  + "\n"
                  + "Full Signature: "
                  + signature);
    }
  }

  private void verifyReturnTypeConsistency(
      final SqlOperator operator, final SimpleExtension.Function funcDef) {
    assertNotNull(operator.getReturnTypeInference(), "Return type inference is null");

    // A. Expected: Evaluate YAML return type -> Convert to Calcite
    final Type yamlReturnType =
        TypeExpressionEvaluator.evaluateExpression(
            funcDef.returnType(), funcDef.args(), Collections.emptyList());
    final RelDataType expectedType = TypeConverter.DEFAULT.toCalcite(TYPE_FACTORY, yamlReturnType);

    // B. Actual: Infer from Operator (using empty binding, sufficient for static types)
    final RelDataType actualType =
        operator
            .getReturnTypeInference()
            .inferReturnType(createMockBinding(operator, Collections.emptyList()));

    // C. Compare
    assertEquals(
        expectedType.getSqlTypeName(),
        actualType.getSqlTypeName(),
        () -> "Return type mismatch for " + funcDef.name());
    assertEquals(
        expectedType.isNullable(),
        actualType.isNullable(),
        () -> "Nullability mismatch for " + funcDef.name());
  }

  private static SqlOperator getOperator(final String name) {
    final SqlOperator op = OPERATORS.get(name.toLowerCase());
    assertNotNull(op, "Operator not found: " + name);
    return op;
  }

  private static SimpleExtension.Function getFunctionDef(final String name) {
    final SimpleExtension.Function func = FUNCTION_DEFS.get(name.toLowerCase());
    assertNotNull(func, "YAML Def not found: " + name);
    return func;
  }

  private SqlOperatorBinding createMockBinding(
      final SqlOperator operator, final List<RelDataType> argumentTypes) {
    return new SqlOperatorBinding(TYPE_FACTORY, operator) {
      @Override
      public int getOperandCount() {
        return argumentTypes.size();
      }

      @Override
      public RelDataType getOperandType(final int ordinal) {
        return argumentTypes.get(ordinal);
      }

      @Override
      public CalciteException newError(final Resources.ExInst<SqlValidatorException> e) {
        return new CalciteException(e.toString(), null);
      }
    };
  }
}
