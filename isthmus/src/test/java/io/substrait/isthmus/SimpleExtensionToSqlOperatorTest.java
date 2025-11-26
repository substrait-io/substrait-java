package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.SimpleExtension;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.junit.jupiter.api.Test;

class SimpleExtensionToSqlOperatorTest {

  @Test
  void test() throws IOException {
    String customFunctionPath = "/extensions/scalar_functions_custom.yaml";

    SimpleExtension.ExtensionCollection customExtensions =
        SimpleExtension.load(
            customFunctionPath,
            SimpleExtensionToSqlOperatorTest.class.getResourceAsStream(customFunctionPath));

    List<SqlOperator> operators = SimpleExtensionToSqlOperator.from(customExtensions);

    Optional<SqlOperator> function =
        operators.stream()
            .filter(op -> op.getName().equalsIgnoreCase("REGEXP_EXTRACT_CUSTOM"))
            .findFirst();

    assertTrue(function.isPresent(), "The REGEXP_EXTRACT_CUSTOM function should be present.");

    SqlOperator op = function.get();
    System.out.println("Successfully found and verified Custom UDF:");
    System.out.printf("  - Name: %s%n", op.getName());

    SqlOperandCountRange operandCountRange = op.getOperandCountRange();
    assertEquals(2, operandCountRange.getMin(), "Function should require 2 arguments.");
    assertEquals(2, operandCountRange.getMax(), "Function should require 2 arguments.");
    System.out.printf("  - Argument Count: %d%n", operandCountRange.getMin());

    assertNotNull(op.getOperandTypeChecker(), "Operand type checker should not be null.");
  }
}
