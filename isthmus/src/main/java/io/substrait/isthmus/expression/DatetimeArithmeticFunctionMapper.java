package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** Maps Calcite datetime arithmetic to Substrait's add and subtract functions. */
final class DatetimeArithmeticFunctionMapper implements ScalarFunctionMapper {
  private final Map<String, List<ScalarFunctionVariant>> datetimeFunctions;
  private final RexBuilder rexBuilder;

  DatetimeArithmeticFunctionMapper(List<ScalarFunctionVariant> functions, RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    datetimeFunctions =
        functions.stream()
            .filter(function -> DefaultExtensionCatalog.FUNCTIONS_DATETIME.equals(function.urn()))
            .filter(function -> "add".equals(function.name()) || "subtract".equals(function.name()))
            .collect(Collectors.groupingBy(ScalarFunctionVariant::name));
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    SqlOperator operator = call.getOperator();
    boolean subtract =
        SqlStdOperatorTable.MINUS_DATE.equals(operator) || operator == SqlStdOperatorTable.MINUS;
    boolean add =
        operator == SqlStdOperatorTable.DATETIME_PLUS || operator == SqlStdOperatorTable.PLUS;
    if (!subtract && !add) {
      return Optional.empty();
    }
    String name = subtract ? "subtract" : "add";
    List<ScalarFunctionVariant> functions = datetimeFunctions.get(name);
    if (functions == null) {
      return Optional.empty();
    }
    List<RexNode> operands = call.getOperands();
    if (operands.size() == 2 && isZonedCalendarArithmetic(operands.get(0), operands.get(1))) {
      // Calendar arithmetic uses the timezone of the session that executes the plan.
      operands =
          List.of(
              operands.get(0),
              operands.get(1),
              rexBuilder.makeCall(CurrentTimezoneFunction.INSTANCE));
    } else if (!SqlStdOperatorTable.MINUS_DATE.equals(operator)) {
      return Optional.empty();
    }
    return Optional.of(new SubstraitFunctionMapping(name, operands, functions));
  }

  private static boolean isZonedCalendarArithmetic(RexNode datetime, RexNode interval) {
    return datetime.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        && interval.getType().getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH;
  }

  @Override
  public Optional<RexNode> toCalcite(
      Expression.ScalarFunctionInvocation expression,
      SqlOperator operator,
      List<RexNode> arguments,
      RelDataType returnType,
      RexBuilder builder) {
    if (DefaultExtensionCatalog.FUNCTIONS_DATETIME.equals(expression.declaration().urn())
        && arguments.size() == 3
        && expression.arguments().get(2) instanceof Expression.CurrentTimezone) {
      if (operator == FunctionMappings.DATETIME_SUBTRACT
          && "subtract:ptstz_iyear_str".equals(expression.declaration().key())) {
        return Optional.of(
            builder.makeCall(returnType, SqlStdOperatorTable.MINUS, arguments.subList(0, 2)));
      }
      if (operator == SqlStdOperatorTable.DATETIME_PLUS
          && "add:ptstz_iyear_str".equals(expression.declaration().key())) {
        return Optional.of(builder.makeCall(returnType, operator, arguments.subList(0, 2)));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      Expression.ScalarFunctionInvocation expression) {
    return Optional.empty();
  }
}
