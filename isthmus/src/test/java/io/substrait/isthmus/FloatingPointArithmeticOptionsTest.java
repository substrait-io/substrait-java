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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.jupiter.api.Test;

class FloatingPointArithmeticOptionsTest extends PlanTestBase {
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  private double execute(RexNode call, double a, double b) throws Exception {
    RexCall original = (RexCall) call;
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    schema.add("inputs", new RuntimeInputs(original, a, b));
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
      return result.getDouble(1);
    }
  }

  private static class RuntimeInputs extends AbstractTable implements ScannableTable {
    private final RexCall call;
    private final Object[] values;

    private RuntimeInputs(RexCall call, double a, double b) {
      this.call = call;
      if (call.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.REAL)
        values = new Object[] {(float) a, (float) b};
      else values = new Object[] {a, b};
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("a", call.getOperands().get(0).getType())
          .add("b", call.getOperands().get(1).getType())
          .build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context) {
      return Linq4j.asEnumerable(new Object[][] {values});
    }
  }

  private Expression.ScalarFunctionInvocation invocation(
      String name, int width, List<FunctionOption> options) {
    Expression zero =
        width == 32 ? ExpressionCreator.fp32(false, 0) : ExpressionCreator.fp64(false, 0);
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        name + ":fp" + width + "_fp" + width,
        width == 32 ? R.FP32 : R.FP64,
        List.of(zero, zero),
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
  void tiesRoundToEvenAndSurviveExport() throws Exception {
    for (int width : List.of(32, 64)) {
      int precision = width == 32 ? 24 : 53;
      double ulp = Math.scalb(1.0, 1 - precision);
      double min = width == 32 ? Float.MIN_VALUE : Double.MIN_VALUE;
      String[] names = {"add", "subtract", "multiply", "divide"};
      double[] left = {1, 1, 1.5, min};
      double[] right = {ulp / 2, ulp / 4, 1 + ulp, 2};
      double[] expected = {1, 1, 1.5 + 2 * ulp, 0};
      for (int i = 0; i < names.length; i++) {
        RexNode rex =
            invocation(names[i], width, List.of(option("rounding", "TIE_TO_EVEN")))
                .accept(toRex, Context.newContext());
        assertEquals(expected[i], execute(rex, left[i], right[i]), width + " " + names[i]);
        Expression.ScalarFunctionInvocation exported = export(rex);
        assertTrue(exported.options().contains(option("rounding", "TIE_TO_EVEN")));
        assertEquals(
            expected[i], execute(exported.accept(toRex, Context.newContext()), left[i], right[i]));
      }
    }
  }

  @Test
  void otherRoundingModesRequireAnEvenFallback() {
    for (int width : List.of(32, 64)) {
      for (String name : List.of("add", "subtract", "multiply", "divide")) {
        for (String unsupported : List.of("TIE_AWAY_FROM_ZERO", "TRUNCATE", "CEILING", "FLOOR")) {
          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  invocation(name, width, List.of(option("rounding", unsupported)))
                      .accept(toRex, Context.newContext()));
          RexNode rex =
              invocation(name, width, List.of(option("ROUNDING", unsupported, "tie_to_even")))
                  .accept(toRex, Context.newContext());
          assertTrue(export(rex).options().contains(option("rounding", "TIE_TO_EVEN")));
        }
      }
    }
  }

  @Test
  void divisionDomainErrorsProduceNan() throws Exception {
    for (int width : List.of(32, 64)) {
      RexNode rex =
          invocation("divide", width, List.of(option("on_domain_error", "NAN")))
              .accept(toRex, Context.newContext());
      for (double[] pair :
          List.of(
              new double[] {Double.NaN, 1},
              new double[] {1, Double.NaN},
              new double[] {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY}))
        assertTrue(Double.isNaN(execute(rex, pair[0], pair[1])));
      assertTrue(export(rex).options().contains(option("on_domain_error", "NAN")));
      for (String unsupported : List.of("NULL", "ERROR")) {
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation("divide", width, List.of(option("on_domain_error", unsupported)))
                    .accept(toRex, Context.newContext()));
        RexNode fallback =
            invocation("divide", width, List.of(option("on_domain_error", unsupported, "NAN")))
                .accept(toRex, Context.newContext());
        assertTrue(Double.isNaN(execute(fallback, Double.NaN, 1)));
      }
    }
  }

  @Test
  void divisionByZeroHasNoUnambiguousOptionInThePinnedSpec() throws Exception {
    for (int width : List.of(32, 64)) {
      for (String preference : List.of("IEEE", "LIMIT", "NULL", "ERROR"))
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation("divide", width, List.of(option("on_division_by_zero", preference)))
                    .accept(toRex, Context.newContext()));
      RexNode rex = invocation("divide", width, List.of()).accept(toRex, Context.newContext());
      assertEquals(Double.POSITIVE_INFINITY, execute(rex, 1, 0));
      assertEquals(Double.NEGATIVE_INFINITY, execute(rex, -1, 0));
      assertTrue(Double.isNaN(execute(rex, 0, 0)));
      assertEquals(Double.doubleToLongBits(-0.0), Double.doubleToLongBits(execute(rex, 0, -1)));
      assertTrue(
          export(rex).options().stream().noneMatch(o -> o.getName().equals("on_division_by_zero")));
    }
  }

  @Test
  void unknownAndEmptyOptionsAreRejected() {
    for (List<FunctionOption> options :
        List.of(
            List.of(option("rounding")),
            List.of(option("overflow", "SILENT")),
            List.of(option("unknown", "TIE_TO_EVEN"))))
      assertThrows(
          UnsupportedOperationException.class,
          () -> invocation("add", 64, options).accept(toRex, Context.newContext()));
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
        custom.getSqlOperatorFromSubstraitFunc(invocation("add", 64, List.of())));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            custom.getSqlOperatorFromSubstraitFunc(
                invocation("add", 64, List.of(option("rounding", "TIE_TO_EVEN")))));
  }

  @Test
  void sqlRoundtripPreservesNativeOptions() throws Exception {
    for (String sqlType : List.of("REAL", "DOUBLE")) {
      assertFullRoundTrip(
          "SELECT a+b, a-b, a*b, a/b FROM numbers",
          "CREATE TABLE numbers (a " + sqlType + ", b " + sqlType + ")");
    }
  }

  @Test
  void sqlExportNamesNativeBehavior() throws Exception {
    for (String sqlType : List.of("REAL", "DOUBLE", "FLOAT")) {
      String query = "SELECT a+b, a-b, a*b, a/b FROM numbers";
      String creates = "CREATE TABLE numbers (a " + sqlType + ", b " + sqlType + ")";
      Project project =
          (Project)
              new SqlToSubstrait()
                  .convert(
                      query,
                      SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates))
                  .getRoots()
                  .get(0)
                  .getInput();
      for (Expression expression : project.getExpressions()) {
        Expression.ScalarFunctionInvocation function =
            (Expression.ScalarFunctionInvocation) expression;
        assertTrue(function.options().contains(option("rounding", "TIE_TO_EVEN")));
        assertEquals(
            function.declaration().name().equals("divide") ? 2 : 1, function.options().size());
      }
    }
  }

  @Test
  void sqlExportNamesNativeOptionsAfterOperandPromotion() throws Exception {
    String query = "SELECT a*2, a+1.5, a+r, a/r FROM numbers";
    Project project =
        (Project)
            new SqlToSubstrait()
                .convert(
                    query,
                    SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                        "CREATE TABLE numbers (a DOUBLE, r REAL)"))
                .getRoots()
                .get(0)
                .getInput();
    for (Expression expression : project.getExpressions()) {
      Expression.ScalarFunctionInvocation function =
          (Expression.ScalarFunctionInvocation) expression;
      assertTrue(function.declaration().key().endsWith(":fp64_fp64"));
      assertTrue(function.options().contains(option("rounding", "TIE_TO_EVEN")));
      if (function.declaration().name().equals("divide")) {
        assertTrue(function.options().contains(option("on_domain_error", "NAN")));
      }
    }
  }
}
