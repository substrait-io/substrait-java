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

class UnaryArithmeticOptionsTest extends PlanTestBase {
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  private double execute(RexNode call, Number a) throws Exception {
    RexCall original = (RexCall) call;
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    schema.add("inputs", new RuntimeInputs(original, a));
    RelBuilder runtimeBuilder = converterProvider.getRelBuilder(schema);
    RelNode input = runtimeBuilder.scan("inputs").build();
    call =
        runtimeBuilder
            .getRexBuilder()
            .makeCall(
                original.getType(),
                original.getOperator(),
                List.of(runtimeBuilder.getRexBuilder().makeInputRef(input, 0)));
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

    private RuntimeInputs(RexCall call, Number a) {
      this.call = call;
      Object value;
      switch (call.getOperands().get(0).getType().getSqlTypeName()) {
        case TINYINT:
          value = a.byteValue();
          break;
        case SMALLINT:
          value = a.shortValue();
          break;
        case INTEGER:
          value = a.intValue();
          break;
        case BIGINT:
          value = a.longValue();
          break;
        case REAL:
          value = a.floatValue();
          break;
        default:
          value = a.doubleValue();
      }
      values = new Object[] {value};
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory.builder().add("a", call.getOperands().get(0).getType()).build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context) {
      return Linq4j.asEnumerable(new Object[][] {values});
    }
  }

  private Type type(String tag) {
    switch (tag) {
      case "i8":
        return R.I8;
      case "i16":
        return R.I16;
      case "i32":
        return R.I32;
      case "i64":
        return R.I64;
      case "fp32":
        return R.FP32;
      default:
        return R.FP64;
    }
  }

  private Expression.ScalarFunctionInvocation invocation(
      String name, String tag, List<FunctionOption> options) {
    Expression zero;
    switch (tag) {
      case "i8":
        zero = ExpressionCreator.i8(false, (byte) 0);
        break;
      case "i16":
        zero = ExpressionCreator.i16(false, (short) 0);
        break;
      case "i32":
        zero = ExpressionCreator.i32(false, 0);
        break;
      case "i64":
        zero = ExpressionCreator.i64(false, 0);
        break;
      case "fp32":
        zero = ExpressionCreator.fp32(false, 0);
        break;
      default:
        zero = ExpressionCreator.fp64(false, 0);
    }
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        name + ":" + tag,
        tag.equals("i64") && (name.equals("sqrt") || name.equals("exp")) ? R.FP64 : type(tag),
        List.of(zero),
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

  private void assertOverflow(RexNode rex, Number value) {
    Exception failure = assertThrows(Exception.class, () -> execute(rex, value));
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    assertTrue(root instanceof ArithmeticException, root.toString());
  }

  @Test
  void negateSupportsCheckedAndSilentOverflowAtEveryWidth() throws Exception {
    for (String tag : List.of("i8", "i16", "i32", "i64")) {
      int width = Integer.parseInt(tag.substring(1));
      long min = width == 64 ? Long.MIN_VALUE : -(1L << (width - 1));
      RexNode checked =
          invocation("negate", tag, List.of(option("overflow", "ERROR")))
              .accept(toRex, Context.newContext());
      assertOverflow(checked, min);
      Expression.ScalarFunctionInvocation exported = export(checked);
      assertEquals(List.of(option("overflow", "ERROR")), exported.options());
      assertOverflow(exported.accept(toRex, Context.newContext()), min);
      RexNode silent =
          invocation("negate", tag, List.of(option("overflow", "SILENT")))
              .accept(toRex, Context.newContext());
      assertEquals((double) min, execute(silent, min));
      assertEquals(List.of(option("overflow", "SILENT")), export(silent).options());
      assertEquals(-2.0, execute(checked, 2));
    }
  }

  @Test
  void preferencesSelectTheFirstSupportedOverflowMode() {
    for (String tag : List.of("i8", "i16", "i32", "i64")) {
      for (List<String> preferences :
          List.of(
              List.of("ERROR", "SILENT"),
              List.of("SILENT", "ERROR"),
              List.of("SATURATE", "error"))) {
        RexCall rex =
            (RexCall)
                invocation(
                        "negate",
                        tag,
                        List.of(
                            FunctionOption.builder().name("OVERFLOW").values(preferences).build()))
                    .accept(toRex, Context.newContext());
        assertEquals(
            preferences.get(0).equals("SILENT")
                ? SqlStdOperatorTable.UNARY_MINUS
                : SqlStdOperatorTable.CHECKED_UNARY_MINUS,
            rex.getOperator());
      }
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              invocation("negate", tag, List.of(option("overflow", "SATURATE")))
                  .accept(toRex, Context.newContext()));
    }
  }

  @Test
  void absOnlySupportsSilentOverflow() throws Exception {
    for (String tag : List.of("i8", "i16", "i32", "i64")) {
      int width = Integer.parseInt(tag.substring(1));
      long min = width == 64 ? Long.MIN_VALUE : -(1L << (width - 1));
      for (String unsupported : List.of("ERROR", "SATURATE")) {
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation("abs", tag, List.of(option("overflow", unsupported)))
                    .accept(toRex, Context.newContext()));
        RexNode rex =
            invocation("abs", tag, List.of(option("overflow", unsupported, "SILENT")))
                .accept(toRex, Context.newContext());
        assertEquals((double) min, execute(rex, min));
        assertEquals(2.0, execute(rex, -2));
        assertEquals(List.of(option("overflow", "SILENT")), export(rex).options());
      }
    }
  }

  @Test
  void asinAndAcosHonorNanDomainPreferences() throws Exception {
    for (String name : List.of("asin", "acos")) {
      for (String tag : List.of("fp64")) {
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation(name, tag, List.of(option("on_domain_error", "ERROR")))
                    .accept(toRex, Context.newContext()));
        RexNode rex =
            invocation(name, tag, List.of(option("on_domain_error", "ERROR", "NAN")))
                .accept(toRex, Context.newContext());
        assertTrue(Double.isNaN(execute(rex, 2)));
        assertTrue(Double.isNaN(execute(rex, Double.NaN)));
        assertEquals(List.of(option("on_domain_error", "NAN")), export(rex).options());
      }
    }
  }

  @Test
  void unprovenRoundingPreferencesAreRejectedForTheWholeUnaryFamily() {
    for (String name :
        List.of(
            "sqrt", "exp", "cos", "sin", "tan", "cosh", "sinh", "tanh", "acos", "asin", "atan",
            "acosh", "asinh", "atanh", "radians", "degrees")) {
      for (String tag : List.of("fp32", "fp64")) {
        for (String rounding :
            List.of("TIE_TO_EVEN", "TIE_AWAY_FROM_ZERO", "TRUNCATE", "CEILING", "FLOOR")) {
          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  invocation(name, tag, List.of(option("rounding", rounding)))
                      .accept(toRex, Context.newContext()),
              name + ":" + tag);
        }
      }
    }
    for (String name : List.of("sqrt", "exp"))
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              invocation(name, "i64", List.of(option("rounding", "TIE_TO_EVEN")))
                  .accept(toRex, Context.newContext()));
  }

  @Test
  void unimplementedDomainAndFactorialPoliciesAreRejected() {
    for (String name : List.of("asin", "acos"))
      for (String domain : List.of("NAN", "ERROR"))
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation(name, "fp32", List.of(option("on_domain_error", domain)))
                    .accept(toRex, Context.newContext()));
    for (String name : List.of("sqrt", "acosh", "atanh"))
      for (String domain : List.of("NAN", "ERROR"))
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation(name, "fp64", List.of(option("on_domain_error", domain)))
                    .accept(toRex, Context.newContext()));
    for (String tag : List.of("i32", "i64"))
      for (String overflow : List.of("SILENT", "SATURATE", "ERROR"))
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                invocation("factorial", tag, List.of(option("overflow", overflow)))
                    .accept(toRex, Context.newContext()));
  }

  @Test
  void unknownEmptyAndConflictingPreferencesAreRejected() {
    for (List<FunctionOption> options :
        List.of(
            List.of(option("overflow")),
            List.of(option("unknown", "SILENT")),
            List.of(option("overflow", "SILENT"), option("overflow", "ERROR"))))
      assertThrows(
          UnsupportedOperationException.class,
          () -> invocation("negate", "i32", options).accept(toRex, Context.newContext()));
  }

  @Test
  void callsWithoutOptionsKeepTheExistingOperators() {
    for (String name :
        List.of(
            "negate", "abs", "sqrt", "exp", "asin", "acos", "acosh", "atanh", "sin", "factorial")) {
      String tag =
          name.equals("factorial") || name.equals("negate") || name.equals("abs") ? "i64" : "fp64";
      RexCall rex = (RexCall) invocation(name, tag, List.of()).accept(toRex, Context.newContext());
      SqlOperator expected =
          io.substrait.isthmus.expression.FunctionMappings.SCALAR_SIGS.stream()
              .filter(sig -> sig.name().equals(name))
              .findFirst()
              .orElseThrow()
              .operator();
      assertEquals(name.equals("sqrt") ? SqlStdOperatorTable.POWER : expected, rex.getOperator());
    }
  }

  @Test
  void customOperatorsDoNotInheritNativeOptions() {
    ScalarFunctionConverter custom =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory) {
          @Override
          public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
              String key, Type outputType) {
            return Optional.of(SqlStdOperatorTable.UNARY_PLUS);
          }
        };
    assertEquals(
        Optional.of(SqlStdOperatorTable.UNARY_PLUS),
        custom.getSqlOperatorFromSubstraitFunc(invocation("negate", "i32", List.of())));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            custom.getSqlOperatorFromSubstraitFunc(
                invocation("negate", "i32", List.of(option("overflow", "SILENT")))));
  }

  @Test
  void sqlExportOnlyNamesVerifiedNativePolicies() throws Exception {
    String creates =
        "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, f DOUBLE)";
    String query =
        "SELECT -i8, -i16, -i32, -i64, abs(i8), abs(i16), abs(i32), abs(i64), asin(f), acos(f), sqrt(f), sin(f), exp(f) FROM numbers";
    assertFullRoundTrip(query, creates);
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
      String name = function.declaration().name();
      if (name.equals("negate") || name.equals("abs"))
        assertEquals(List.of(option("overflow", "SILENT")), function.options());
      else if (name.equals("asin") || name.equals("acos"))
        assertEquals(List.of(option("on_domain_error", "NAN")), function.options());
      else assertTrue(function.options().isEmpty(), name);
    }
  }
}
