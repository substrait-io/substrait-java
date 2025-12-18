package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom mapping for the Calcite MONTH/DAY/QUARTER functions.
 *
 * <p>These come from Calcite as 2 argument functions (like YEAR) but in Substrait these functions
 * are 3 arguments; the additional being if this is a 0 or 1 based value. Calcite is a 1 based value
 * in this case.
 *
 * <p>We need to therefore map the MONTH etc functions to a different Substrait function.
 */
final class ExtractDateFunctionMapper implements ScalarFunctionMapper {

  private final Map<String, ScalarFunctionVariant> extractFunctions;

  public ExtractDateFunctionMapper(List<ScalarFunctionVariant> functions) {

    Map<String, ScalarFunctionVariant> fns =
        functions.stream()
            .filter(f -> "extract".equals(f.name()))
            .collect(Collectors.toMap(Object::toString, f -> f));

    this.extractFunctions = Collections.unmodifiableMap(fns);
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(final RexCall call) {
    if (!SqlStdOperatorTable.EXTRACT.equals(call.op)) {
      return Optional.empty();
    }

    if (call.operandCount() < 2) {
      return Optional.empty();
    }

    RexNode extractType = call.operands.get(0);
    if (extractType.getType().getSqlTypeName() != SqlTypeName.SYMBOL) {
      return Optional.empty();
    }

    final RexNode dataType = call.operands.get(1);
    if (!dataType.getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
      return Optional.empty();
    }

    TimeUnitRange value = ((RexLiteral) extractType).getValueAs(TimeUnitRange.class);

    switch (value) {
      case QUARTER:
      case MONTH:
      case DAY:
        {
          final List<RexNode> newOperands = new LinkedList<>(call.operands);
          newOperands.add(1, RexBuilder.DEFAULT.makeFlag(ExtractIndexing.ONE));

          final ScalarFunctionVariant substraitFn =
              this.extractFunctions.get("extract:req_req_date");
          return Optional.of(
              new SubstraitFunctionMapping("extract", newOperands, List.of(substraitFn)));
        }
      default:
        return Optional.empty();
    }
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      final Expression.ScalarFunctionInvocation expression) {
    String name = expression.declaration().toString();

    if ("extract:req_req_date".equals(name)) {
      final List<FunctionArg> newArgs = new LinkedList<>(expression.arguments());
      newArgs.remove(1);

      return Optional.of(newArgs);
    }
    return Optional.empty();
  }
}
