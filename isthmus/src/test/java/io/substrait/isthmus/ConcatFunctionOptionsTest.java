package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.DataContexts;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConcatFunctionOptionsTest extends PlanTestBase {
  private final ScalarFunctionConverter scalar =
      new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory);
  private final WindowFunctionConverter window =
      new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(typeFactory, scalar, window, TypeConverter.DEFAULT);
  private final RexExpressionConverter fromRex =
      new RexExpressionConverter(
          null,
          Stream.concat(CallConverters.defaults(TypeConverter.DEFAULT).stream(), Stream.of(scalar))
              .toList(),
          window,
          TypeConverter.DEFAULT);

  private Expression.ScalarFunctionInvocation concat(List<FunctionOption> options) {
    Expression.ScalarFunctionInvocation call =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "concat:str",
            N.STRING,
            ExpressionCreator.string(false, "a"),
            ExpressionCreator.typedNull(N.STRING));
    return Expression.ScalarFunctionInvocation.builder().from(call).options(options).build();
  }

  private FunctionOption nullHandling(String... preferences) {
    return FunctionOption.builder().name("null_handling").addValues(preferences).build();
  }

  @ParameterizedTest
  @ValueSource(strings = {"concat(a, b)", "a || b", "concat(a, b, a)"})
  void sqlExportNamesItsNullHandling(String expression) throws Exception {
    Plan plan =
        new SqlToSubstrait()
            .convert(
                "SELECT " + expression + " FROM strings",
                SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                    "CREATE TABLE strings (a VARCHAR, b VARCHAR)"));
    Expression.ScalarFunctionInvocation call =
        (Expression.ScalarFunctionInvocation)
            ((Project) plan.getRoots().get(0).getInput()).getExpressions().get(0);
    assertEquals(List.of(nullHandling("ACCEPT_NULLS")), call.options());
  }

  @Test
  void roundTripKeepsAnExplicitOption() {
    Expression.ScalarFunctionInvocation original = concat(List.of(nullHandling("ACCEPT_NULLS")));
    RexNode rex = original.accept(toRex, Context.newContext());
    Expression.ScalarFunctionInvocation back =
        (Expression.ScalarFunctionInvocation) rex.accept(fromRex);
    assertEquals(original.options(), back.options());
  }

  @Test
  void acceptNullsReturnsNull() {
    RexNode accept =
        concat(List.of(nullHandling("ACCEPT_NULLS"))).accept(toRex, Context.newContext());
    List<RexNode> reduced = new ArrayList<>();
    new RexExecutorImpl(DataContexts.EMPTY).reduce(creator.rex(), List.of(accept), reduced);
    assertTrue(RexLiteral.isNullLiteral(reduced.get(0)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"IGNORE_NULLS", "IGNORE_NULLS,ACCEPT_NULLS", "ACCEPT_NULLS,IGNORE_NULLS"})
  void selectsTheFirstSupportedPreference(String preferences) {
    Expression.ScalarFunctionInvocation original =
        concat(List.of(nullHandling(preferences.split(","))));
    if (preferences.equals("IGNORE_NULLS")) {
      UnsupportedOperationException failure =
          assertThrows(
              UnsupportedOperationException.class,
              () -> original.accept(toRex, Context.newContext()));
      assertTrue(failure.getMessage().contains("ACCEPT_NULLS"));
    } else {
      RexNode rex = original.accept(toRex, Context.newContext());
      Expression.ScalarFunctionInvocation back =
          (Expression.ScalarFunctionInvocation) rex.accept(fromRex);
      assertEquals(List.of(nullHandling("ACCEPT_NULLS")), back.options());
    }
  }

  @Test
  void matchesOptionNamesAndValuesCaseInsensitively() {
    FunctionOption option =
        FunctionOption.builder().name("NULL_HANDLING").addValues("accept_nulls").build();
    RexNode rex = concat(List.of(option)).accept(toRex, Context.newContext());
    Expression.ScalarFunctionInvocation back =
        (Expression.ScalarFunctionInvocation) rex.accept(fromRex);
    assertEquals(List.of(nullHandling("ACCEPT_NULLS")), back.options());
  }

  @Test
  void omittedOptionsAllowCalciteToChooseItsSupportedBehavior() {
    RexNode rex = concat(List.of()).accept(toRex, Context.newContext());
    Expression.ScalarFunctionInvocation back =
        (Expression.ScalarFunctionInvocation) rex.accept(fromRex);
    assertEquals(List.of(nullHandling("ACCEPT_NULLS")), back.options());
  }

  @Test
  void rejectsAnEmptyPreferenceList() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> concat(List.of(nullHandling())).accept(toRex, Context.newContext()));
  }

  @Test
  void rejectsAnUnknownOption() {
    FunctionOption option = FunctionOption.builder().name("unknown").addValues("value").build();
    UnsupportedOperationException failure =
        assertThrows(
            UnsupportedOperationException.class,
            () -> concat(List.of(option)).accept(toRex, Context.newContext()));
    assertTrue(failure.getMessage().contains("unknown"));
  }

  @Test
  void leavesAnotherExtensionsConcatOptionsToItsMapper() {
    Expression.ScalarFunctionInvocation original = concat(List.of(nullHandling("IGNORE_NULLS")));
    SimpleExtension.ScalarFunctionVariant custom =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .from(original.declaration())
            .urn("extension:org.example:strings")
            .build();
    Expression.ScalarFunctionInvocation call =
        Expression.ScalarFunctionInvocation.builder().from(original).declaration(custom).build();
    assertEquals(call.arguments(), scalar.getExpressionArguments(call));
  }
}
