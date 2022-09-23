package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.isthmus.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

public class CallConverters {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CallConverters.class);

  public static SimpleCallConverter CAST =
      (call, visitor) -> {
        if (call.getKind() != SqlKind.CAST) {
          return null;
        }

        return ExpressionCreator.cast(
            TypeConverter.convert(call.getType()), visitor.apply(call.getOperands().get(0)));
      };

  //  public static SimpleCallConverter OrAnd(FunctionConverter c) {
  //      return (call, visitor) -> {
  //        if (call.getKind() != SqlKind.AND && call.getKind() != SqlKind.OR) {
  //          return null;
  //        }
  //
  //
  //        return null;
  //      };
  //  }
  /** */
  public static SimpleCallConverter CASE =
      (call, visitor) -> {
        if (call.getKind() != SqlKind.CASE) {
          return null;
        }

        // number of arguments are always going to be odd (each condition/then combination plus
        // else)
        assert call.getOperands().size() % 2 == 1;

        var caseArgs = call.getOperands().stream().map(visitor).toList();

        var last = caseArgs.size() - 1;
        // for if/else, process in reverse to maintain query order
        var caseConditions = new ArrayList<Expression.IfClause>();
        for (int i = 0; i < last; i += 2) {
          caseConditions.add(
              ImmutableExpression.IfClause.builder()
                  .condition(caseArgs.get(i))
                  .then(caseArgs.get(i + 1))
                  .build());
        }

        var defaultResult = caseArgs.get(last);
        return ExpressionCreator.ifThenStatement(defaultResult, caseConditions);
      };

    /**
     * Convert 'or' call to substrait singleOrList in case of calcite optimize the in expression as or expression.
     */
    public static SimpleCallConverter SINGLE_OR_LIST = (call, visitor) -> {
        if (call.getKind() != SqlKind.OR) {
          return null;
        }

        List<RexNode> options = new ArrayList<>();
        RexNode condition = null;
        for (RexNode operand : call.getOperands()) {
          if (!operand.isA(SqlKind.EQUALS)) {
            return null;
          }
          assert operand instanceof RexCall;
          RexCall leftEQCall = (RexCall) operand;
          assert leftEQCall.getOperands().size() == 2;

          RexNode eqCallLeft = leftEQCall.getOperands().get(0);
          RexNode eqCallRight = leftEQCall.getOperands().get(1);
          if (condition == null) {
            condition = eqCallLeft;
          } else {
            if (!condition.equals(eqCallLeft)) {
              return null;
            }
          }
          options.add(eqCallRight);
        }

        return Expression.SingleOrList
                .builder()
                .condition(visitor.apply(condition))
                .options(options.stream().map(visitor::apply).toList())
                .build();
    };

  /**
   * Expand {@link org.apache.calcite.util.Sarg} values in a calcite `SqlSearchOperator` into
   * simpler expressions. The expansion logic is encoded in {@link RexUtil#expandSearch(RexBuilder,
   * RexProgram, RexNode)}
   */
  public static Function<RexBuilder, SimpleCallConverter> CREATE_SEARCH_CONV =
      (RexBuilder rexBuilder) ->
          (RexCall call, Function<RexNode, Expression> visitor) -> {
            if (call.getKind() != SqlKind.SEARCH) {
              return null;
            } else {
              var expandSearch = RexUtil.expandSearch(rexBuilder, null, call);
              // if no expansion happened, avoid infinite recursion.
              return expandSearch.equals(call) ? null : visitor.apply(expandSearch);
            }
          };

  public static final List<CallConverter> DEFAULTS =
      ImmutableList.of(
          new FieldSelectionConverter(),
          CallConverters.CASE,
          CallConverters.CAST,
          CallConverters.SINGLE_OR_LIST,
          new LiteralConstructorConverter());

  public interface SimpleCallConverter extends CallConverter {

    @Nullable
    Expression apply(RexCall call, Function<RexNode, Expression> topLevelConverter);

    @Override
    default Optional<Expression> convert(
        RexCall call, Function<RexNode, Expression> topLevelConverter) {
      return Optional.ofNullable(apply(call, topLevelConverter));
    }
  }
}
