package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.type.SubstraitUserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.jspecify.annotations.Nullable;

public class CallConverters {

  public static Function<TypeConverter, SimpleCallConverter> CAST =
      typeConverter ->
          (call, visitor) -> {
            Expression.FailureBehavior failureBehavior;
            switch (call.getKind()) {
              case CAST:
                failureBehavior = Expression.FailureBehavior.THROW_EXCEPTION;
                break;
              case SAFE_CAST:
                failureBehavior = Expression.FailureBehavior.RETURN_NULL;
                break;
              default:
                return null;
            }

            return ExpressionCreator.cast(
                typeConverter.toSubstrait(call.getType()),
                visitor.apply(call.getOperands().get(0)),
                failureBehavior);
          };

  /**
   * {@link SqlKind#REINTERPRET} is utilized by Isthmus to represent {@link
   * Expression.UserDefinedAny} literals within Calcite.
   *
   * <p>When converting from Substrait to Calcite, UserDefinedAny literals are serialized to binary
   * and stored as {@link org.apache.calcite.sql.type.SqlTypeName#BINARY} {@link
   * org.apache.calcite.rex.RexLiteral}, then re-interpreted to have a custom {@link
   * SubstraitUserDefinedType.SubstraitUserDefinedAnyType} that preserves all metadata including
   * type parameters.
   *
   * <p>Note: {@link Expression.UserDefinedStruct} literals are NOT handled via REINTERPRET.
   * Instead, they are represented as Calcite ROW literals with {@link
   * SubstraitUserDefinedType.SubstraitUserDefinedStructType} and converted via {@link
   * LiteralConverter}.
   *
   * <p>See {@link ExpressionRexConverter#visit(Expression.UserDefinedAny,
   * SubstraitRelNodeConverter.Context)} for the UserDefinedAny conversion.
   *
   * <p>When converting from Calcite back to Substrait, this call converter deserializes the binary
   * value and reconstructs the UserDefinedAny literal with all metadata preserved (including type
   * parameters).
   */
  public static Function<TypeConverter, SimpleCallConverter> REINTERPRET =
      typeConverter ->
          (call, visitor) -> {
            if (call.getKind() != SqlKind.REINTERPRET) {
              return null;
            }
            Expression operand = visitor.apply(call.getOperands().get(0));

            if (operand instanceof Expression.FixedBinaryLiteral
                && call.getType() instanceof SubstraitUserDefinedType.SubstraitUserDefinedAnyType) {
              Expression.FixedBinaryLiteral literal = (Expression.FixedBinaryLiteral) operand;
              SubstraitUserDefinedType.SubstraitUserDefinedAnyType customType =
                  (SubstraitUserDefinedType.SubstraitUserDefinedAnyType) call.getType();

              try {
                com.google.protobuf.Any anyValue =
                    com.google.protobuf.Any.parseFrom(literal.value().toByteArray());

                return Expression.UserDefinedAny.builder()
                    .urn(customType.getUrn())
                    .name(customType.getName())
                    .typeParameters(customType.getTypeParameters())
                    .value(anyValue)
                    .nullable(customType.isNullable())
                    .build();
              } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                    "Failed to parse UserDefinedAny literal value", e);
              }
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

        List<Expression> caseArgs =
            call.getOperands().stream().map(visitor).collect(java.util.stream.Collectors.toList());

        int last = caseArgs.size() - 1;
        // for if/else, process in reverse to maintain query order
        List<Expression.IfClause> caseConditions = new ArrayList<>();
        for (int i = 0; i < last; i += 2) {
          caseConditions.add(
              Expression.IfClause.builder()
                  .condition(caseArgs.get(i))
                  .then(caseArgs.get(i + 1))
                  .build());
        }

        Expression defaultResult = caseArgs.get(last);
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
              RexNode expandSearch = RexUtil.expandSearch(rexBuilder, null, call);
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

    @Nullable Expression apply(RexCall call, Function<RexNode, Expression> topLevelConverter);

    @Override
    default Optional<Expression> convert(
        RexCall call, Function<RexNode, Expression> topLevelConverter) {
      return Optional.ofNullable(apply(call, topLevelConverter));
    }
  }
}
