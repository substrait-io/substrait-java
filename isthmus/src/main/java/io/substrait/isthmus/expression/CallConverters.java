package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.jspecify.annotations.Nullable;

/**
 * Collection of small, composable {@link CallConverter}s for common Calcite {@link RexCall}s (e.g.,
 * CAST, CASE, REINTERPRET, SEARCH). Each converter returns a Substrait {@link Expression} or {@code
 * null} when the call is not handled.
 *
 * <p>Use {@link #defaults(TypeConverter)} to get a standard set.
 */
public class CallConverters {

  /**
   * Converter for {@link SqlKind#CAST} and {@link SqlKind#SAFE_CAST} to Substrait {@link
   * Expression.Cast}.
   *
   * <p>On SAFE_CAST, sets {@link Expression.FailureBehavior#RETURN_NULL}; otherwise
   * THROW_EXCEPTION.
   *
   * @see ExpressionCreator#cast(Type, Expression, Expression.FailureBehavior)
   */
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
   * {@link SqlKind#REINTERPRET} is utilized by Isthmus to represent and store {@link
   * Expression.UserDefinedLiteral}s within Calcite.
   *
   * <p>When converting from Substrait to Calcite, the user-defined literal value is stored either
   * as a {@link org.apache.calcite.sql.type.SqlTypeName#BINARY} {@link
   * org.apache.calcite.rex.RexLiteral} (for ANY-encoded values) or a {@link SqlKind#ROW} (for
   * struct-encoded values) and then re-interpreted to have the correct user-defined type.
   *
   * <p>See {@link ExpressionRexConverter#visit(Expression.UserDefinedAnyLiteral,
   * SubstraitRelNodeConverter.Context)} and {@link
   * ExpressionRexConverter#visit(Expression.UserDefinedStructLiteral,
   * SubstraitRelNodeConverter.Context)} for this conversion.
   *
   * <p>When converting from Calcite to Substrait, this call converter extracts the stored {@link
   * Expression.UserDefinedLiteral}.
   */
  public static Function<TypeConverter, SimpleCallConverter> REINTERPRET =
      typeConverter ->
          (call, visitor) -> {
            if (call.getKind() != SqlKind.REINTERPRET) {
              return null;
            }
            Expression operand = visitor.apply(call.getOperands().get(0));
            Type type = typeConverter.toSubstrait(call.getType());

            // Calcite encoded Expression.UserDefinedAnyLiteral
            if (operand instanceof Expression.FixedBinaryLiteral
                && type instanceof Type.UserDefined) {
              Expression.FixedBinaryLiteral literal = (Expression.FixedBinaryLiteral) operand;
              Type.UserDefined t = (Type.UserDefined) type;

              // The binary literal contains the serialized protobuf Any - just parse it directly
              try {
                com.google.protobuf.Any anyValue =
                    com.google.protobuf.Any.parseFrom(literal.value().toByteArray());

                return Expression.UserDefinedAnyLiteral.builder()
                    .nullable(t.nullable())
                    .urn(t.urn())
                    .name(t.name())
                    .addAllTypeParameters(t.typeParameters())
                    .value(anyValue)
                    .build();
              } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalStateException("Failed to parse UserDefinedAnyLiteral value", e);
              }
            }
            // Calcite encoded Expression.UserDefinedStructLiteral
            else if (operand instanceof Expression.StructLiteral
                && type instanceof Type.UserDefined) {
              Expression.StructLiteral structLiteral = (Expression.StructLiteral) operand;
              Type.UserDefined t = (Type.UserDefined) type;

              return Expression.UserDefinedStructLiteral.builder()
                  .nullable(t.nullable())
                  .urn(t.urn())
                  .name(t.name())
                  .addAllTypeParameters(t.typeParameters())
                  .addAllFields(structLiteral.fields())
                  .build();
            }
            return null;
          };

  /**
   * Converts Calcite ROW constructors into Substrait {@link Expression.StructLiteral}s.
   *
   * <p>ROW values are always concrete (never null themselves) - if a value is actually null, use
   * NullLiteral instead of StructLiteral. Therefore, the resulting StructLiteral always has
   * nullable=false. The ROW's type may be nullable (for regular structs) or non-nullable (for UDT
   * struct encoding), but the value itself is always concrete.
   *
   * <p>Each literal's nullability is set to match its field type's nullability.
   */
  public static SimpleCallConverter ROW =
      (call, visitor) -> {
        if (call.getKind() != SqlKind.ROW) {
          return null;
        }

        List<Expression> operands =
            call.getOperands().stream().map(visitor).collect(Collectors.toList());
        if (!operands.stream().allMatch(expr -> expr instanceof Expression.Literal)) {
          throw new IllegalArgumentException("ROW operands must be literals.");
        }

        // ROW types are never nullable (struct literals are always concrete values).
        // Field nullability comes from individual field types, so match literal nullability
        // to field type nullability.
        List<RelDataTypeField> fieldTypes = call.getType().getFieldList();
        List<Expression.Literal> literals =
            java.util.stream.IntStream.range(0, operands.size())
                .mapToObj(
                    i -> {
                      Expression.Literal lit = (Expression.Literal) operands.get(i);
                      boolean fieldIsNullable = fieldTypes.get(i).getType().isNullable();
                      return lit.withNullable(fieldIsNullable);
                    })
                .collect(Collectors.toList());

        // Struct literals are always concrete values (never null).
        // For UDT struct literals, struct-level nullability is in the REINTERPRET target type.
        return ExpressionCreator.struct(false, literals);
      };

  /**
   * Converter for {@link SqlKind#CASE} expressions to a Substrait {@link Expression.IfThen}.
   *
   * <p>The Calcite {@code CASE} call is expected to have an odd number of operands, arranged as
   * alternating {@code WHEN}/{@code THEN} pairs followed by a final {@code ELSE} expression:
   *
   * <pre>
   *   CASE
   *     WHEN cond1 THEN result1
   *     WHEN cond2 THEN result2
   *     ...
   *     ELSE defaultResult
   *   END
   * </pre>
   *
   * <p>Each {@code WHEN}/{@code THEN} pair is converted into an {@link Expression.IfClause}, and
   * the final operand becomes the default (else) expression. The order of conditions is preserved
   * to maintain Calcite evaluation semantics.
   *
   * <p>This converter assumes that operand expressions have already been converted by the provided
   * top-level visitor.
   */
  public static SimpleCallConverter CASE =
      (call, visitor) -> {
        if (call.getKind() != SqlKind.CASE) {
          return null;
        }

        // number of arguments are always going to be odd (each condition/then combination plus
        // else)
        assert call.getOperands().size() % 2 == 1;

        List<Expression> caseArgs =
            call.getOperands().stream().map(visitor).collect(Collectors.toList());

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
   *
   * <p>Returns a {@link SimpleCallConverter} that expands SEARCH calls using the provided {@link
   * RexBuilder}
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

  /**
   * Returns the default set of converters for common calls.
   *
   * @param typeConverter type mapper between Substrait and Calcite types
   * @return list of default {@link CallConverter}s
   */
  public static List<CallConverter> defaults(TypeConverter typeConverter) {
    return ImmutableList.of(
        new FieldSelectionConverter(typeConverter),
        CallConverters.CASE,
        CallConverters.ROW,
        CallConverters.CAST.apply(typeConverter),
        CallConverters.REINTERPRET.apply(typeConverter),
        new SqlArrayValueConstructorCallConverter(typeConverter),
        new SqlMapValueConstructorCallConverter());
  }

  /** Minimal interface for single-call converters used by {@link CallConverter}. */
  public interface SimpleCallConverter extends CallConverter {

    /**
     * Converts a given {@link RexCall} to a Substrait {@link Expression}, or returns {@code null}
     * if not handled.
     *
     * @param call the Calcite call to convert
     * @param topLevelConverter converter for nested {@link RexNode} operands
     * @return converted expression, or {@code null} if not applicable
     */
    @Nullable Expression apply(RexCall call, Function<RexNode, Expression> topLevelConverter);

    /**
     * Default adapter to CallConverter#apply(RexCall, Function).
     *
     * @param call the Calcite call to convert
     * @param topLevelConverter converter for nested {@link RexNode} operands
     * @return optional converted expression
     */
    @Override
    default Optional<Expression> convert(
        RexCall call, Function<RexNode, Expression> topLevelConverter) {
      return Optional.ofNullable(apply(call, topLevelConverter));
    }
  }
}
