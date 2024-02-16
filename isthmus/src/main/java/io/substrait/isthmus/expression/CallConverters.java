package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.isthmus.*;
import io.substrait.type.Type;
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

  public static Function<TypeConverter, SimpleCallConverter> CAST =
      typeConverter ->
          (call, visitor) -> {
            if (call.getKind() != SqlKind.CAST) {
              return null;
            }

            return ExpressionCreator.cast(
                typeConverter.toSubstrait(call.getType()),
                visitor.apply(call.getOperands().get(0)));
          };

  public static Function<TypeConverter, SimpleCallConverter> REINTERPRET =
      typeConverter ->
          (call, visitor) -> {
            if (call.getKind() != SqlKind.REINTERPRET) {
              return null;
            }

            var operand = visitor.apply(call.getOperands().get(0));
            var type = typeConverter.toSubstrait(call.getType());

            // for now, we only support reinterpretation of fixed binary literals to user defined
            // type literals
            // this is a needed workaround as calcite does not support user defined type literals
            // and has
            // strict type checking for literals, specifically checking if the value matches the
            // calcite.sql.type.SqlTypeName
            // note: This is tightly coupled to
            // ExpressionRexConverter.visit(Expression.UserDefinedLiteral expr)
            // if we ever start accepting other ways to encode user defined type literals (e.g.
            // structured UDTs), this will need to be updated
            if (operand instanceof Expression.FixedBinaryLiteral literal
                && type instanceof Type.UserDefined t) {
              return Expression.UserDefinedLiteral.builder()
                  .uri(t.uri())
                  .name(t.name())
                  .value(literal.value())
                  .build();
            }
            return null;
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

        var caseArgs =
            call.getOperands().stream().map(visitor).collect(java.util.stream.Collectors.toList());

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

  public static List<CallConverter> defaults(TypeConverter typeConverter) {
    return ImmutableList.of(
        new FieldSelectionConverter(typeConverter),
        CallConverters.CASE,
        CallConverters.CAST.apply(typeConverter),
        CallConverters.REINTERPRET.apply(typeConverter),
        new LiteralConstructorConverter(typeConverter));
  }

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
