package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import java.util.List;
import org.apache.calcite.runtime.SqlFunctions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ContainsFunctionMappingTest extends PlanTestBase {
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  @ParameterizedTest
  @ValueSource(strings = {"", "CASE_SENSITIVE", "CASE_INSENSITIVE", "CASE_INSENSITIVE_ASCII"})
  void standardContainsIsNotMappedToContainsSubstr(String preference) {
    List<FunctionOption> options =
        preference.isEmpty()
            ? List.of()
            : List.of(
                FunctionOption.builder().name("case_sensitivity").addValues(preference).build());
    Expression.ScalarFunctionInvocation call =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "contains:str_str",
            R.BOOLEAN,
            List.of(
                ExpressionCreator.string(false, "{\"key\":\"value\"}"),
                ExpressionCreator.string(false, "key")),
            options);
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> call.accept(toRex, Context.newContext()));
    assertTrue(failure.getMessage().contains("contains"));
  }

  @Test
  void jsonInterpretationCannotBeExpressedByCaseSensitivity() {
    // The key is literally present regardless of case sensitivity, but Calcite treats this
    // string as a JSON object and searches only its values. Spec v0.103.0 contains searches
    // the input string, not the parsed object's values.
    String input = "{\"key\":\"value\"}";
    assertTrue(input.contains("key"));
    assertEquals(false, SqlFunctions.containsSubstr(input, "key"));
  }

  @Test
  void automaticMappingsDoNotRestoreTheContainsSubstrBinding() throws Exception {
    ConverterProvider provider =
        new AutomaticDynamicFunctionMappingConverterProvider(ConverterProvider.builder());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SqlToSubstrait(provider)
                .convert(
                    "SELECT contains_substr(a, 'key') FROM strings",
                    SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                        "CREATE TABLE strings (a VARCHAR)")));
  }
}
