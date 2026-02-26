package io.substrait.isthmus.expression;

import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * Abstract base class for custom mappings between Calcite PARSE_* functions and Substrait
 * strptime_* functions.
 *
 * <p>Calcite PARSE_* functions have <em>format</em> followed by <em>string</em> parameters, while
 * Substrait strptime_* functions have <em>string</em> followed by <em>format</em>. When mapping
 * between Calcite and Substrait, the parameters need to be reversed.
 */
abstract class AbstractStrptimeFunctionMapper implements ScalarFunctionMapper {
  private final String substraitFunctionName;
  private final SqlOperator calciteOperator;
  private final List<ScalarFunctionVariant> strptimeFunctions;

  /**
   * Constructs an abstract strptime function mapper.
   *
   * @param substraitFunctionName the name of the Substrait function (e.g., "strptime_date")
   * @param calciteOperator the Calcite operator to map from (e.g., SqlLibraryOperators.PARSE_DATE)
   * @param functions the list of all available scalar function variants
   */
  protected AbstractStrptimeFunctionMapper(
      String substraitFunctionName,
      SqlOperator calciteOperator,
      List<ScalarFunctionVariant> functions) {
    this.substraitFunctionName = substraitFunctionName;
    this.calciteOperator = calciteOperator;
    this.strptimeFunctions =
        functions.stream()
            .filter(f -> substraitFunctionName.equals(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (!calciteOperator.equals(call.op)) {
      return Optional.empty();
    }

    List<RexNode> operands = new ArrayList<>(call.getOperands());
    Collections.swap(operands, 0, 1);

    return Optional.of(
        new SubstraitFunctionMapping(substraitFunctionName, operands, strptimeFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(ScalarFunctionInvocation expression) {
    if (!substraitFunctionName.equals(expression.declaration().name())) {
      return Optional.empty();
    }

    List<FunctionArg> arguments = new ArrayList<>(expression.arguments());
    Collections.swap(arguments, 0, 1);

    return Optional.of(arguments);
  }
}
