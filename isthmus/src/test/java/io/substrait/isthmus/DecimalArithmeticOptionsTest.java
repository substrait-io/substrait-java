package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.Project;
import io.substrait.type.Type;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.jupiter.api.Test;

class DecimalArithmeticOptionsTest extends PlanTestBase {
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  private BigDecimal execute(RexNode call) throws Exception {
    RexCall original = (RexCall) call;
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    schema.add("inputs", new RuntimeInputs(original));
    RelBuilder runtimeBuilder = converterProvider.getRelBuilder(schema);
    RelNode input = runtimeBuilder.scan("inputs").build();
    call =
        runtimeBuilder
            .getRexBuilder()
            .makeCall(
                original.getType(),
                original.getOperator(),
                List.of(
                    runtimeBuilder.getRexBuilder().makeInputRef(input, 0),
                    runtimeBuilder.getRexBuilder().makeInputRef(input, 1)));
    RelNode project = LogicalProject.create(input, List.of(), List.of(call), List.of("result"));
    try (PreparedStatement statement = RelRunners.run(project);
        ResultSet result = statement.executeQuery()) {
      if (!result.next()) {
        throw new IllegalStateException("No result row");
      }
      return result.getBigDecimal(1);
    }
  }

  private static class RuntimeInputs extends AbstractTable implements ScannableTable {
    private final RexCall call;

    private RuntimeInputs(RexCall call) {
      this.call = call;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("a", call.getOperands().get(0).getType())
          .add("b", call.getOperands().get(1).getType())
          .build();
    }

    private BigDecimal value(RexNode expression) {
      return ((RexLiteral) expression).getValueAs(BigDecimal.class);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context) {
      return Linq4j.asEnumerable(
          new Object[][] {{value(call.getOperands().get(0)), value(call.getOperands().get(1))}});
    }
  }

  private Expression.ScalarFunctionInvocation invocation(
      String name, String a, String b, List<FunctionOption> options) {
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
        name + ":dec_dec",
        R.decimal(38, name.equals("divide") ? 6 : 0),
        List.of(
            ExpressionCreator.decimal(false, new BigDecimal(a), 38, 0),
            ExpressionCreator.decimal(false, new BigDecimal(b), 38, 0)),
        options);
  }

  private FunctionOption option(String name, String... values) {
    return FunctionOption.builder().name(name).addValues(values).build();
  }

  private Expression.ScalarFunctionInvocation export(RexNode rex) {
    ScalarFunctionConverter scalar =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory);
    RexExpressionConverter converter =
        new RexExpressionConverter(
            null,
            Stream.concat(
                    CallConverters.defaults(TypeConverter.DEFAULT).stream(), Stream.of(scalar))
                .toList(),
            new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
            TypeConverter.DEFAULT);
    return (Expression.ScalarFunctionInvocation) rex.accept(converter);
  }

  @Test
  void uncheckedOverflowSurvivesImportAndExport() throws Exception {
    String max = "9".repeat(38);
    String[] names = {"add", "subtract", "multiply", "divide"};
    String[] left = {max, "-" + max, max, max};
    String[] right = {"1", "1", "2", "1"};
    for (int i = 0; i < names.length; i++) {
      Expression.ScalarFunctionInvocation expression =
          invocation(names[i], left[i], right[i], List.of(option("overflow", "SILENT")));
      RexNode rex = expression.accept(toRex, Context.newContext());
      BigDecimal value = execute(rex);
      int scale = names[i].equals("divide") ? 6 : 0;
      BigDecimal bound = BigDecimal.TEN.pow(38 - scale);
      assertTrue(value.abs().compareTo(bound) >= 0, names[i]);
      Expression.ScalarFunctionInvocation exported = export(rex);
      assertEquals(expression.options(), exported.options());
      assertEquals(0, value.compareTo(execute(exported.accept(toRex, Context.newContext()))));
    }
  }

  @Test
  void unsupportedOverflowRequiresSilentFallback() throws Exception {
    for (String name : List.of("add", "subtract", "multiply", "divide")) {
      for (String unsupported : List.of("ERROR", "SATURATE")) {
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation(name, "1", "2", List.of(option("overflow", unsupported)))
                    .accept(toRex, Context.newContext()));
        RexNode rex =
            invocation(name, "1", "2", List.of(option("OVERFLOW", unsupported, "silent")))
                .accept(toRex, Context.newContext());
        assertEquals(List.of(option("overflow", "SILENT")), export(rex).options());
        assertEquals(
            0,
            execute(rex)
                .compareTo(
                    execute(
                        invocation(name, "1", "2", List.of())
                            .accept(toRex, Context.newContext()))));
      }
    }
  }

  @Test
  void modulusCannotOverflowForValidOperands() throws Exception {
    for (String preference : List.of("SILENT", "SATURATE", "ERROR")) {
      for (String a : List.of("9".repeat(38), "-" + "9".repeat(38))) {
        RexNode rex =
            invocation("modulus", a, "7", List.of(option("overflow", preference)))
                .accept(toRex, Context.newContext());
        assertEquals(0, new BigDecimal(a).remainder(new BigDecimal("7")).compareTo(execute(rex)));
        assertEquals(List.of(option("overflow", "SILENT")), export(rex).options());
      }
    }
  }

  @Test
  void omittedOptionsKeepTheExistingOperator() {
    RexCall rex =
        (RexCall) invocation("add", "1", "2", List.of()).accept(toRex, Context.newContext());
    assertEquals(SqlStdOperatorTable.PLUS, rex.getOperator());
  }

  @Test
  void unknownEmptyAndConflictingOptionsAreRejected() {
    for (List<FunctionOption> options :
        List.of(
            List.of(option("unknown", "SILENT")),
            List.of(option("overflow")),
            List.of(option("overflow", "unknown")))) {
      assertThrows(
          UnsupportedOperationException.class,
          () -> invocation("add", "1", "2", options).accept(toRex, Context.newContext()));
    }
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            invocation(
                    "modulus",
                    "1",
                    "2",
                    List.of(option("overflow", "SILENT"), option("overflow", "ERROR")))
                .accept(toRex, Context.newContext()));
  }

  @Test
  void customOperatorDoesNotInheritTheNativePolicy() {
    ScalarFunctionConverter custom =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory) {
          @Override
          public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
              String key, Type outputType) {
            return Optional.of(SqlStdOperatorTable.CHECKED_PLUS);
          }
        };
    assertEquals(
        Optional.of(SqlStdOperatorTable.CHECKED_PLUS),
        custom.getSqlOperatorFromSubstraitFunc(invocation("add", "1", "2", List.of())));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            custom.getSqlOperatorFromSubstraitFunc(
                invocation("add", "1", "2", List.of(option("overflow", "SILENT")))));
  }

  @Test
  void sqlExportNamesUncheckedBehavior() throws Exception {
    for (String sqlType : List.of("DECIMAL(38,0)", "DECIMAL(10,2)")) {
      assertFullRoundTrip(
          "SELECT a+b, a-b, a*b, a/b, mod(a,b) FROM numbers",
          "CREATE TABLE numbers (a " + sqlType + ", b " + sqlType + ")");
      Project project =
          (Project)
              new SqlToSubstrait()
                  .convert(
                      "SELECT a+b, a-b, a*b, a/b, mod(a,b) FROM numbers",
                      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                          "CREATE TABLE numbers (a " + sqlType + ", b " + sqlType + ")"))
                  .getRoots()
                  .get(0)
                  .getInput();
      for (Expression expression : project.getExpressions()) {
        Expression.ScalarFunctionInvocation function =
            (Expression.ScalarFunctionInvocation) expression;
        assertEquals(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, function.declaration().urn());
        assertEquals(List.of(option("overflow", "SILENT")), function.options());
      }
    }
  }

  @Test
  void integerOperandsDoNotLoseDecimalOverflowOptions() throws Exception {
    String query = "SELECT a+b, a-b, a*b, a/b, mod(a,b) FROM numbers";
    String creates = "CREATE TABLE numbers (a DECIMAL(38,2), b INTEGER)";
    Project project =
        (Project)
            new SqlToSubstrait()
                .convert(
                    query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates))
                .getRoots()
                .get(0)
                .getInput();
    for (Expression expression : project.getExpressions()) {
      Expression.ScalarFunctionInvocation function =
          (Expression.ScalarFunctionInvocation) expression;
      assertEquals(
          DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, function.declaration().urn());
      assertEquals(List.of(option("overflow", "SILENT")), function.options());
    }
    assertFullRoundTrip(query, creates);
  }

  @Test
  void undeclaredPreferencesCannotHideBehindASupportedFallback() {
    for (String[] values :
        List.of(new String[] {"WRAP", "SILENT"}, new String[] {"SILENT", "WRAP"})) {
      UnsupportedOperationException failure =
          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  invocation("add", "1", "2", List.of(option("overflow", values)))
                      .accept(toRex, Context.newContext()));
      assertTrue(failure.getMessage().contains("does not declare value WRAP"));
    }
    assertTrue(
        export(
                invocation("add", "1", "2", List.of(option("overflow", "silent")))
                    .accept(toRex, Context.newContext()))
            .options()
            .contains(option("overflow", "SILENT")));
  }
}
