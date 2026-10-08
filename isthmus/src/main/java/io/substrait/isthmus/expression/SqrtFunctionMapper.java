package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom function mapper to represent power(x, 0.5) as sqrt(x) to ensure the float formats are
 * supported as defined in substrait/extensions/functions_arithmetic
 */
final class SqrtFunctionMapper implements ScalarFunctionMapper {
  private static final String sqrtFunctionName = "sqrt";
  private final List<ScalarFunctionVariant> sqrtFunctions;
  private final RexBuilder rexBuilder;

  public SqrtFunctionMapper(List<ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    this.rexBuilder = new RexBuilder(typeFactory);
    this.sqrtFunctions =
        functions.stream()
            .filter(f -> sqrtFunctionName.equalsIgnoreCase(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (sqrtFunctions.isEmpty()) {
      return Optional.empty();
    }

    if (isPowerOfHalf(call)) {
      List<ScalarFunctionVariant> candidates = sqrtFunctions;
      RexNode input = call.getOperands().get(0);
      if (input.getKind() == SqlKind.CAST) {
        RexNode uncast = ((RexCall) input).getOperands().get(0);
        SqlTypeName source = uncast.getType().getSqlTypeName();
        if (input.getType().getSqlTypeName() == SqlTypeName.DOUBLE
            && source == SqlTypeName.BIGINT
            && call.getType().getSqlTypeName() == SqlTypeName.DOUBLE
            && input.getType().isNullable() == uncast.getType().isNullable()
            && sqrtFunctions.stream()
                .anyMatch(
                    function ->
                        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn())
                            && function.key().equals("sqrt:i64"))) {
          // The reverse mapping restores this input promotion. Keep the original
          // arithmetic variant when the executable expansion is exported again.
          input = uncast;
          candidates =
              sqrtFunctions.stream()
                  .filter(
                      function ->
                          DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn()))
                  .collect(Collectors.toUnmodifiableList());
        }
      }
      if (input.getType().getSqlTypeName() == SqlTypeName.REAL
          && call.getType().getSqlTypeName() == SqlTypeName.DOUBLE) {
        // Calcite POWER returns DOUBLE even for REAL input. Match sqrt:fp64
        // rather than declaring an FP64 result for the FP32 signature.
        RelDataType promotedType =
            rexBuilder
                .getTypeFactory()
                .createTypeWithNullability(
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
                    input.getType().isNullable());
        input = rexBuilder.makeCast(promotedType, input);
      }
      List<RexNode> operands = List.of(input);
      return Optional.of(new SubstraitFunctionMapping(sqrtFunctionName, operands, candidates));
    }

    return Optional.empty();
  }

  static Optional<RexNode> toCalcite(
      Expression.ScalarFunctionInvocation expression,
      SqlOperator operator,
      List<RexNode> arguments,
      RelDataType returnType,
      RexBuilder rexBuilder) {
    if (operator != SqlStdOperatorTable.SQRT
        || !expression.options().isEmpty()
        || !DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(expression.declaration().urn())
        || !List.of("sqrt:i64", "sqrt:fp32", "sqrt:fp64").contains(expression.declaration().key())
        || arguments.size() != 1) return Optional.empty();
    RexNode half = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(0.5));
    RexNode input = arguments.get(0);
    // POWER has no float/double overload. Promote first to avoid Calcite selecting
    // a BigDecimal overload, which cannot carry NaN or infinity.
    RelDataType doubleType =
        rexBuilder
            .getTypeFactory()
            .createTypeWithNullability(half.getType(), input.getType().isNullable());
    RexNode promoted = rexBuilder.makeCast(doubleType, input);
    RexNode power = rexBuilder.makeCall(SqlStdOperatorTable.POWER, promoted, half);
    return Optional.of(rexBuilder.makeCast(returnType, power));
  }

  private static boolean isPowerOfHalf(final RexCall call) {
    if (!SqlStdOperatorTable.POWER.equals(call.getOperator()) || call.getOperands().size() != 2) {
      return false;
    }

    RexNode exponent = call.getOperands().get(1);
    while (exponent.getKind() == SqlKind.CAST) {
      SqlTypeName target = exponent.getType().getSqlTypeName();
      if (target != SqlTypeName.DOUBLE
          && target != SqlTypeName.FLOAT
          && target != SqlTypeName.REAL
          && !(target == SqlTypeName.DECIMAL && exponent.getType().getScale() >= 1)) {
        return false;
      }
      exponent = ((RexCall) exponent).getOperands().get(0);
    }

    if (!(exponent instanceof RexLiteral)) {
      return false;
    }
    RexLiteral literal = (RexLiteral) exponent;

    switch (literal.getType().getSqlTypeName()) {
      case DOUBLE:
      case FLOAT:
      case REAL:
        {
          final Double digit = literal.getValueAs(Double.class);
          return digit != null && digit == 0.5d;
        }

      case DECIMAL:
        {
          final BigDecimal bigdec = literal.getValueAs(BigDecimal.class);
          return bigdec != null && BigDecimal.valueOf(5, 1).compareTo(bigdec) == 0;
        }

      default:
        return false;
    }
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      final Expression.ScalarFunctionInvocation expression) {
    return Optional.empty();
  }
}
