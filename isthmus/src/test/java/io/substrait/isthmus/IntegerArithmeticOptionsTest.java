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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.jupiter.api.Test;

class IntegerArithmeticOptionsTest extends PlanTestBase {
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  private String execute(RexNode call) {
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
      return "VALUE=" + result.getObject(1);
    } catch (Exception | LinkageError failure) {
      Throwable root = failure;
      while (root.getCause() != null) root = root.getCause();
      if (root instanceof NullPointerException) {
        throw new IllegalStateException("Probe execution failed", failure);
      }
      return "ERROR=" + root.getClass().getSimpleName() + ": " + root.getMessage();
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
          .add(
              "a",
              factory.createTypeWithNullability(
                  call.getOperands().get(0).getType(), call.getType().isNullable()))
          .add(
              "b",
              factory.createTypeWithNullability(
                  call.getOperands().get(1).getType(), call.getType().isNullable()))
          .build();
    }

    private Object value(RexNode expression) {
      RexLiteral literal = (RexLiteral) expression;
      switch (literal.getType().getSqlTypeName()) {
        case TINYINT:
          return literal.getValueAs(Integer.class).byteValue();
        case SMALLINT:
          return literal.getValueAs(Integer.class).shortValue();
        case INTEGER:
          return literal.getValueAs(Integer.class);
        case BIGINT:
          return literal.getValueAs(Long.class);
        default:
          return literal.getValueAs(BigDecimal.class);
      }
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context) {
      return Linq4j.asEnumerable(
          new Object[][] {{value(call.getOperands().get(0)), value(call.getOperands().get(1))}});
    }
  }

  private Expression integer(int width, long value) {
    switch (width) {
      case 8:
        return ExpressionCreator.i8(false, (byte) value);
      case 16:
        return ExpressionCreator.i16(false, (short) value);
      case 32:
        return ExpressionCreator.i32(false, (int) value);
      default:
        return ExpressionCreator.i64(false, value);
    }
  }

  private Type integerType(int width) {
    return width == 8 ? R.I8 : width == 16 ? R.I16 : width == 32 ? R.I32 : R.I64;
  }

  private Expression.ScalarFunctionInvocation invocation(
      String name, int width, long a, long b, List<FunctionOption> options) {
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        name + ":i" + width + "_i" + width,
        integerType(width),
        List.of(integer(width, a), integer(width, b)),
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
  void checkedOverflowIsExecutedAndSurvivesExportForEveryWidth() {
    for (int width : List.of(8, 16, 32, 64)) {
      long max = width == 64 ? Long.MAX_VALUE : (1L << (width - 1)) - 1;
      long min = width == 64 ? Long.MIN_VALUE : -(1L << (width - 1));
      String[] names = {"add", "subtract", "multiply", "divide"};
      long[] left = {max, min, max, min};
      long[] right = {1, 1, 2, -1};
      long[] wrapped = {min, max, -2, min};
      for (int i = 0; i < names.length; i++) {
        Expression.ScalarFunctionInvocation expression =
            invocation(names[i], width, left[i], right[i], List.of(option("overflow", "ERROR")));
        RexNode rex = expression.accept(toRex, Context.newContext());
        assertTrue(execute(rex).startsWith("ERROR=ArithmeticException"), width + " " + names[i]);
        assertTrue(export(rex).options().contains(option("overflow", "ERROR")));
        Expression.ScalarFunctionInvocation silent =
            invocation(names[i], width, left[i], right[i], List.of(option("overflow", "SILENT")));
        if (width <= 16) {
          assertThrows(
              UnsupportedOperationException.class,
              () -> silent.accept(toRex, Context.newContext()));
        } else {
          assertEquals("VALUE=" + wrapped[i], execute(silent.accept(toRex, Context.newContext())));
        }
      }
    }
  }

  @Test
  void narrowOverflowDependsOnResultNullabilityUnlessTheOperatorIsChecked() {
    for (int width : List.of(8, 16)) {
      long max = (1L << (width - 1)) - 1;
      long min = -(1L << (width - 1));
      String[] names = {"add", "subtract", "multiply", "divide"};
      long[] left = {max, min, max, min};
      long[] right = {1, 1, 2, -1};
      long[] wrapped = {min, max, -2, min};
      for (int i = 0; i < names.length; i++) {
        for (boolean nullable : List.of(false, true)) {
          Type type =
              width == 8 ? Type.withNullability(nullable).I8 : Type.withNullability(nullable).I16;
          Expression.ScalarFunctionInvocation plain =
              Expression.ScalarFunctionInvocation.builder()
                  .from(invocation(names[i], width, left[i], right[i], List.of()))
                  .outputType(type)
                  .build();
          RexNode call = plain.accept(toRex, Context.newContext());
          String result = execute(call);
          if (nullable) assertEquals("VALUE=" + wrapped[i], result, names[i]);
          else assertTrue(result.startsWith("ERROR=ArithmeticException"), result);
          assertTrue(
              export(call).options().stream().noneMatch(o -> o.getName().equals("overflow")));
          Expression.ScalarFunctionInvocation checked =
              Expression.ScalarFunctionInvocation.builder()
                  .from(plain)
                  .addOptions(option("overflow", "ERROR"))
                  .build();
          RexNode checkedCall = checked.accept(toRex, Context.newContext());
          assertTrue(execute(checkedCall).startsWith("ERROR=ArithmeticException"), names[i]);
          assertTrue(export(checkedCall).options().contains(option("overflow", "ERROR")));
        }
      }
    }
  }

  @Test
  void preferenceOrderChoosesTheFirstSupportedBehavior() {
    for (List<String> preferences :
        List.of(
            List.of("ERROR", "SILENT"), List.of("SILENT", "ERROR"), List.of("SATURATE", "ERROR"))) {
      RexNode rex =
          invocation(
                  "add",
                  32,
                  Integer.MAX_VALUE,
                  1,
                  List.of(FunctionOption.builder().name("OVERFLOW").values(preferences).build()))
              .accept(toRex, Context.newContext());
      String result = execute(rex);
      if (preferences.get(0).equals("SILENT")) assertEquals("VALUE=" + Integer.MIN_VALUE, result);
      else assertTrue(result.startsWith("ERROR=ArithmeticException"));
    }
    RexNode rex =
        invocation("add", 32, 1, 2, List.of(option("overflow", "error")))
            .accept(toRex, Context.newContext());
    assertEquals("VALUE=3", execute(rex));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            invocation("add", 32, 1, 2, List.of(option("overflow", "SATURATE")))
                .accept(toRex, Context.newContext()));
  }

  @Test
  void zeroDivisionAndModulusRejectNullAndAcceptErrorFallback() {
    for (String name : List.of("divide", "modulus")) {
      String optionName = name.equals("divide") ? "on_division_by_zero" : "on_domain_error";
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              invocation(name, 32, 1, 0, List.of(option(optionName, "NULL")))
                  .accept(toRex, Context.newContext()));
      RexNode rex =
          invocation(name, 32, 1, 0, List.of(option(optionName, "NULL", "ERROR")))
              .accept(toRex, Context.newContext());
      assertTrue(execute(rex).startsWith("ERROR=ArithmeticException"));
    }
  }

  @Test
  void modulusRejectsFloorAndKeepsTruncation() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            invocation("modulus", 32, -5, 3, List.of(option("division_type", "FLOOR")))
                .accept(toRex, Context.newContext()));
    RexNode rex =
        invocation("modulus", 32, -5, 3, List.of(option("division_type", "FLOOR", "TRUNCATE")))
            .accept(toRex, Context.newContext());
    assertEquals("VALUE=-2", execute(rex));
    assertTrue(export(rex).options().contains(option("division_type", "TRUNCATE")));
    for (int width : List.of(8, 16, 32, 64)) {
      long min = width == 64 ? Long.MIN_VALUE : -(1L << (width - 1));
      RexNode remainder =
          invocation("modulus", width, min, -1, List.of(option("overflow", "ERROR")))
              .accept(toRex, Context.newContext());
      assertEquals("VALUE=0", execute(remainder));
    }
  }

  @Test
  void omittedOptionsKeepTheExistingOperator() {
    RexCall rex =
        (RexCall)
            invocation("add", 32, Integer.MAX_VALUE, 1, List.of())
                .accept(toRex, Context.newContext());
    assertEquals(SqlStdOperatorTable.PLUS, rex.getOperator());
    assertEquals("VALUE=" + Integer.MIN_VALUE, execute(rex));
  }

  @Test
  void unknownEmptyAndConflictingOptionsAreRejected() {
    for (List<FunctionOption> options :
        List.of(
            List.of(option("unknown", "ERROR")),
            List.of(option("overflow")),
            List.of(option("overflow", "ERROR"), option("overflow", "SILENT")))) {
      assertThrows(
          UnsupportedOperationException.class,
          () -> invocation("add", 32, 1, 2, options).accept(toRex, Context.newContext()));
    }
  }

  @Test
  void sqlExportNamesTheActualNativeBehavior() throws Exception {
    for (int width : List.of(8, 16, 32, 64)) {
      String sqlType =
          width == 8 ? "TINYINT" : width == 16 ? "SMALLINT" : width == 32 ? "INT" : "BIGINT";
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
        if (width <= 16 && !function.declaration().name().equals("modulus")) {
          assertTrue(function.options().stream().noneMatch(o -> o.getName().equals("overflow")));
        } else {
          assertTrue(function.options().contains(option("overflow", "SILENT")));
        }
        if (function.declaration().name().equals("divide")) {
          assertTrue(function.options().contains(option("on_division_by_zero", "ERROR")));
        }
      }
    }
  }
}
