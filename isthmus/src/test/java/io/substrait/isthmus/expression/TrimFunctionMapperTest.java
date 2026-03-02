package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.Expression.StrLiteral;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.junit.jupiter.api.Test;

class TrimFunctionMapperTest extends PlanTestBase {
  final TrimFunctionMapper trimFunctionMapper =
      new TrimFunctionMapper(DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions());

  final RexBuilder rexBuilder = builder.getRexBuilder();

  @Test
  void calciteTrimBothArgumentOrder() {
    final RexNode characters = rexBuilder.makeLiteral(" ");
    final RexNode input = rexBuilder.makeLiteral("  whitespace  ");
    final RexNode trimBothRexCall =
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            List.of(rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH), characters, input));

    Optional<SubstraitFunctionMapping> substraitCall =
        trimFunctionMapper.toSubstrait((RexCall) trimBothRexCall);

    assertEquals("trim", substraitCall.get().substraitName());
    // operands should be swapped now
    assertEquals(input, substraitCall.get().operands().get(0));
    assertEquals(characters, substraitCall.get().operands().get(1));
  }

  @Test
  void calciteTrimLeadingArgumentOrder() {
    final RexNode characters = rexBuilder.makeLiteral(" ");
    final RexNode input = rexBuilder.makeLiteral("  whitespace  ");
    final RexNode trimBothRexCall =
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            List.of(rexBuilder.makeFlag(SqlTrimFunction.Flag.LEADING), characters, input));

    Optional<SubstraitFunctionMapping> substraitCall =
        trimFunctionMapper.toSubstrait((RexCall) trimBothRexCall);

    assertEquals("ltrim", substraitCall.get().substraitName());
    // operands should be swapped now
    assertEquals(input, substraitCall.get().operands().get(0));
    assertEquals(characters, substraitCall.get().operands().get(1));
  }

  @Test
  void calciteTrimTrailingArgumentOrder() {
    final RexNode characters = rexBuilder.makeLiteral(" ");
    final RexNode input = rexBuilder.makeLiteral("  whitespace  ");
    final RexNode trimBothRexCall =
        rexBuilder.makeCall(
            SqlStdOperatorTable.TRIM,
            List.of(rexBuilder.makeFlag(SqlTrimFunction.Flag.TRAILING), characters, input));

    Optional<SubstraitFunctionMapping> substraitCall =
        trimFunctionMapper.toSubstrait((RexCall) trimBothRexCall);

    assertEquals("rtrim", substraitCall.get().substraitName());
    // operands should be swapped now
    assertEquals(input, substraitCall.get().operands().get(0));
    assertEquals(characters, substraitCall.get().operands().get(1));
  }

  @Test
  void substraitTrimArgumentOrder() {
    final StrLiteral characters = sb.str(" ");
    final StrLiteral input = sb.str("  whitespace  ");
    ScalarFunctionInvocation trimFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "trim:str_str",
            TypeCreator.REQUIRED.STRING,
            input,
            characters);

    Optional<List<FunctionArg>> arguments = trimFunctionMapper.getExpressionArguments(trimFn);

    assertEquals(EnumArg.of(SqlTrimFunction.Flag.BOTH.name()), arguments.get().get(0));
    // arguments should be swapped now
    assertEquals(characters, arguments.get().get(1));
    assertEquals(input, arguments.get().get(2));
  }

  @Test
  void substraitLtrimArgumentOrder() {
    final StrLiteral characters = sb.str(" ");
    final StrLiteral input = sb.str("  whitespace  ");
    ScalarFunctionInvocation trimFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "ltrim:str_str",
            TypeCreator.REQUIRED.STRING,
            input,
            characters);

    Optional<List<FunctionArg>> arguments = trimFunctionMapper.getExpressionArguments(trimFn);

    assertEquals(EnumArg.of(SqlTrimFunction.Flag.LEADING.name()), arguments.get().get(0));
    // arguments should be swapped now
    assertEquals(characters, arguments.get().get(1));
    assertEquals(input, arguments.get().get(2));
  }

  @Test
  void substraitRtrimArgumentOrder() {
    final StrLiteral characters = sb.str(" ");
    final StrLiteral input = sb.str("  whitespace  ");
    ScalarFunctionInvocation trimFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "rtrim:str_str",
            TypeCreator.REQUIRED.STRING,
            input,
            characters);

    Optional<List<FunctionArg>> arguments = trimFunctionMapper.getExpressionArguments(trimFn);

    assertEquals(EnumArg.of(SqlTrimFunction.Flag.TRAILING.name()), arguments.get().get(0));
    // arguments should be swapped now
    assertEquals(characters, arguments.get().get(1));
    assertEquals(input, arguments.get().get(2));
  }
}
