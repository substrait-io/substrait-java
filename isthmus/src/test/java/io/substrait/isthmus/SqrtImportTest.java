package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.substrait.relation.Rel;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.jupiter.api.Test;

class SqrtImportTest extends PlanTestBase {
  private ExpressionRexConverter converter(ScalarFunctionConverter scalar) {
    return new ExpressionRexConverter(
        typeFactory,
        scalar,
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
  }

  private Type inputType(String tag, boolean nullable) {
    return tag.equals("i64")
        ? Type.withNullability(nullable).I64
        : tag.equals("fp32")
            ? Type.withNullability(nullable).FP32
            : Type.withNullability(nullable).FP64;
  }

  private Type outputType(String tag, boolean nullable) {
    return tag.equals("fp32")
        ? Type.withNullability(nullable).FP32
        : Type.withNullability(nullable).FP64;
  }

  private Expression.ScalarFunctionInvocation expression(String tag, boolean nullable) {
    Rel input = sb.namedScan(List.of("inputs"), List.of("a"), List.of(inputType(tag, nullable)));
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        "sqrt:" + tag,
        outputType(tag, nullable),
        sb.fieldReference(input, 0));
  }

  private record RuntimePlan(RexNode call, RelNode input) {}

  private RuntimePlan plan(String tag, Object value) {
    boolean nullable = value == null;
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    Type inputType = inputType(tag, nullable);
    schema.add("inputs", new AbstractInputs(inputType, value));
    RelBuilder builder = converterProvider.getRelBuilder(schema);
    RelNode input = builder.scan("inputs").build();
    RexNode call =
        expression(tag, nullable)
            .accept(
                converter(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)),
                Context.newContext());
    return new RuntimePlan(call, input);
  }

  private static class AbstractInputs extends AbstractTable implements ScannableTable {
    private final Type type;
    private final Object value;

    private AbstractInputs(Type type, Object value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory.builder().add("a", TypeConverter.DEFAULT.toCalcite(factory, type)).build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext context) {
      return Linq4j.asEnumerable(new Object[][] {{value}});
    }
  }

  private Object execute(RuntimePlan plan) throws Exception {
    RelNode project =
        LogicalProject.create(plan.input(), List.of(), List.of(plan.call()), List.of("result"));
    try (PreparedStatement statement = RelRunners.run(project);
        ResultSet result = statement.executeQuery()) {
      assertTrue(result.next());
      return result.getObject(1);
    }
  }

  private Expression export(RuntimePlan plan) {
    ScalarFunctionConverter scalar =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory);
    return export(plan, scalar);
  }

  private Expression export(RuntimePlan plan, ScalarFunctionConverter scalar) {
    return plan.call()
        .accept(
            new RexExpressionConverter(
                null,
                Stream.concat(
                        CallConverters.defaults(TypeConverter.DEFAULT).stream(), Stream.of(scalar))
                    .toList(),
                new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
                TypeConverter.DEFAULT));
  }

  @Test
  void everyArithmeticVariantExecutesAndPreservesItsDeclaredType() throws Exception {
    for (String tag : List.of("i64", "fp32", "fp64")) {
      Object value = tag.equals("i64") ? (Object) 9L : tag.equals("fp32") ? (Object) 9.0f : 9.0;
      RuntimePlan plan = plan(tag, value);
      assertEquals(
          outputType(tag, false), TypeConverter.DEFAULT.toSubstrait(plan.call().getType()));
      assertEquals(3.0, ((Number) execute(plan)).doubleValue());
      Expression exported = export(plan);
      assertEquals(outputType(tag, false), exported.getType());
      RexNode imported =
          exported.accept(
              converter(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)),
              Context.newContext());
      assertEquals(3.0, ((Number) execute(new RuntimePlan(imported, plan.input()))).doubleValue());
    }
  }

  @Test
  void floatingPointSpecialValuesAndNullExecute() throws Exception {
    for (String tag : List.of("fp32", "fp64")) {
      for (double value : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY, -0.0}) {
        Object input = tag.equals("fp32") ? (Object) (float) value : value;
        double actual = ((Number) execute(plan(tag, input))).doubleValue();
        double expected = Math.pow(value, 0.5);
        assertEquals(expected, actual);
      }
    }
    for (String tag : List.of("i64", "fp32", "fp64")) {
      RuntimePlan plan = plan(tag, null);
      assertEquals(outputType(tag, true), TypeConverter.DEFAULT.toSubstrait(plan.call().getType()));
      assertEquals(null, execute(plan));
    }
  }

  @Test
  void onlyAnExactHalfExponentIsExportedAsSqrt() {
    for (double exponent : new double[] {0.5, Math.nextUp(0.5), Math.nextDown(0.5)}) {
      RexNode call =
          builder
              .getRexBuilder()
              .makeCall(
                  SqlStdOperatorTable.POWER,
                  builder
                      .getRexBuilder()
                      .makeInputRef(TypeConverter.DEFAULT.toCalcite(typeFactory, R.FP64), 0),
                  builder.getRexBuilder().makeApproxLiteral(BigDecimal.valueOf(exponent)));
      Expression.ScalarFunctionInvocation exported =
          (Expression.ScalarFunctionInvocation) export(new RuntimePlan(call, null));
      assertEquals(exponent == 0.5 ? "sqrt" : "power", exported.declaration().name());
    }
  }

  @Test
  void castsThatCannotPreserveHalfAreNotExportedAsSqrt() {
    RexNode half = builder.getRexBuilder().makeApproxLiteral(BigDecimal.valueOf(0.5));
    for (RelDataType narrowing :
        List.of(
            typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER),
            typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 0))) {
      RexNode exponent = builder.getRexBuilder().makeAbstractCast(narrowing, half);
      exponent = builder.getRexBuilder().makeAbstractCast(half.getType(), exponent);
      RexNode call =
          builder
              .getRexBuilder()
              .makeCall(
                  SqlStdOperatorTable.POWER,
                  builder.getRexBuilder().makeInputRef(half.getType(), 0),
                  exponent);
      Expression.ScalarFunctionInvocation exported =
          (Expression.ScalarFunctionInvocation) export(new RuntimePlan(call, null));
      assertEquals("power", exported.declaration().name());
    }
  }

  @Test
  void aLimitedCatalogKeepsAnExplicitWideningCast() {
    ScalarFunctionConverter scalar =
        new ScalarFunctionConverter(
            extensions.scalarFunctions().stream()
                .filter(
                    function ->
                        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn())
                            && function.key().equals("sqrt:fp64"))
                .toList(),
            typeFactory);
    RexNode input =
        builder
            .getRexBuilder()
            .makeInputRef(TypeConverter.DEFAULT.toCalcite(typeFactory, R.I64), 0);
    RexNode promoted =
        builder
            .getRexBuilder()
            .makeCast(TypeConverter.DEFAULT.toCalcite(typeFactory, R.FP64), input);
    RexNode call =
        builder
            .getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.POWER,
                promoted,
                builder.getRexBuilder().makeApproxLiteral(BigDecimal.valueOf(0.5)));
    Expression.ScalarFunctionInvocation exported =
        (Expression.ScalarFunctionInvocation) export(new RuntimePlan(call, null), scalar);
    assertEquals("sqrt:fp64", exported.declaration().key());
    assertTrue(exported.arguments().get(0) instanceof Expression.Cast);
  }

  @Test
  void explicitOptionsAreNotReinterpretedByThePowerExpansion() {
    Expression expression =
        Expression.ScalarFunctionInvocation.builder()
            .from(expression("fp64", false))
            .addOptions(FunctionOption.builder().name("on_domain_error").addValues("ERROR").build())
            .build();
    RexCall call =
        (RexCall)
            expression.accept(
                converter(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)),
                Context.newContext());
    assertEquals(SqlStdOperatorTable.SQRT, call.getOperator());
  }

  @Test
  void customOperatorIsNotRewritten() {
    ScalarFunctionConverter custom =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory) {
          @Override
          public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
              String key, Type outputType) {
            return Optional.of(SqlStdOperatorTable.UNARY_PLUS);
          }
        };
    RexCall call =
        (RexCall) expression("fp64", false).accept(converter(custom), Context.newContext());
    assertEquals(SqlStdOperatorTable.UNARY_PLUS, call.getOperator());
    assertEquals(1, call.getOperands().size());
  }

  @Test
  void decimalSqrtKeepsItsExistingMapping() {
    Expression expression =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
            "sqrt:dec",
            R.FP64,
            ExpressionCreator.decimal(false, new BigDecimal("4.00"), 10, 2));
    RexCall call =
        (RexCall)
            expression.accept(
                converter(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)),
                Context.newContext());
    assertEquals(SqlStdOperatorTable.SQRT, call.getOperator());
  }
}
