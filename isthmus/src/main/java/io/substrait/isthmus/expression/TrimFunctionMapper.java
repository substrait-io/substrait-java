package io.substrait.isthmus.expression;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom mapping for the Calcite TRIM function to various Substrait functions. The first TRIM
 * operand indicates the Substrait function to which it should be mapped. The first operand is then
 * omitted from the arguments supplied to the Substrait function.
 *
 * <ul>
 *   <li>TRIM('BOTH', characters, string) -> trim(characters, string)
 *   <li>TRIM('LEADING', characters, string) -> ltrim(characters, string)
 *   <li>TRIM('TRAILING', .characters, string) -> rtrim(characters, string)
 * </ul>
 */
final class TrimFunctionMapper implements ScalarFunctionMapper {

  private enum Trim {
    TRIM("trim", SqlTrimFunction.Flag.BOTH),
    LTRIM("ltrim", SqlTrimFunction.Flag.LEADING),
    RTRIM("rtrim", SqlTrimFunction.Flag.TRAILING);

    private final String substraitName;
    private final SqlTrimFunction.Flag flag;

    Trim(String substraitName, SqlTrimFunction.Flag flag) {
      this.substraitName = substraitName;
      this.flag = flag;
    }

    public String substraitName() {
      return substraitName;
    }

    public SqlTrimFunction.Flag flag() {
      return flag;
    }

    public static Optional<Trim> fromFlag(SqlTrimFunction.Flag flag) {
      return Arrays.stream(values()).filter(t -> t.flag == flag).findAny();
    }

    public static Optional<Trim> fromSubstraitName(String name) {
      return Arrays.stream(values()).filter(t -> t.substraitName.equals(name)).findAny();
    }
  }

  private final Map<Trim, List<ScalarFunctionVariant>> trimFunctions;

  public TrimFunctionMapper(List<ScalarFunctionVariant> functions) {
    Map<Trim, List<ScalarFunctionVariant>> trims = new HashMap<>();
    for (Trim t : Trim.values()) {
      List<ScalarFunctionVariant> funcs = findFunction(t.substraitName(), functions);
      if (!funcs.isEmpty()) {
        trims.put(t, funcs);
      }
    }
    trimFunctions = Collections.unmodifiableMap(trims);
  }

  private List<ScalarFunctionVariant> findFunction(
      String name, Collection<ScalarFunctionVariant> functions) {
    return functions.stream()
        .filter(f -> name.equals(f.name()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(final RexCall call) {
    if (!SqlStdOperatorTable.TRIM.equals(call.op)) {
      return Optional.empty();
    }

    Optional<Trim> trimType = getTrimCallType(call);

    return trimType.map(
        trim -> {
          List<ScalarFunctionVariant> functions = trimFunctions.getOrDefault(trim, List.of());
          if (functions.isEmpty()) {
            return null;
          }

          String name = trim.substraitName();
          List<RexNode> operands =
              call.getOperands().stream().skip(1).collect(Collectors.toUnmodifiableList());
          return new SubstraitFunctionMapping(name, operands, functions);
        });
  }

  private Optional<Trim> getTrimCallType(RexCall call) {
    RexNode trimType = call.operands.get(0);
    if (trimType.getType().getSqlTypeName() != SqlTypeName.SYMBOL) {
      return Optional.empty();
    }

    Comparable value = ((RexLiteral) trimType).getValue();
    if (!(value instanceof SqlTrimFunction.Flag)) {
      return Optional.empty();
    }

    return Trim.fromFlag((SqlTrimFunction.Flag) value);
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      final Expression.ScalarFunctionInvocation expression) {
    String name = expression.declaration().name();
    return Trim.fromSubstraitName(name)
        .map(Trim::flag)
        .map(SqlTrimFunction.Flag::name)
        .map(EnumArg::of)
        .map(
            trimTypeArg -> {
              LinkedList args = new LinkedList<>(expression.arguments());
              args.addFirst(trimTypeArg);
              return args;
            });
  }
}
