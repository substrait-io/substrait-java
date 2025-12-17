package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Rel;
import io.substrait.type.StringTypeVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Converts Calcite {@link RexNode} trees to Substrait {@link Expression}s.
 *
 * <p>Delegates function calls to registered {@link CallConverter}s and supports window function
 * conversion via {@link WindowFunctionConverter}. Some Rex node kinds are intentionally unsupported
 * and will throw {@link UnsupportedOperationException}.
 */
public class RexExpressionConverter implements RexVisitor<Expression> {

  private final List<CallConverter> callConverters;
  private final SubstraitRelVisitor relVisitor;
  private final TypeConverter typeConverter;
  private WindowFunctionConverter windowFunctionConverter;

  /**
   * Creates a converter with an explicit {@link SubstraitRelVisitor} and one or more call
   * converters.
   *
   * @param relVisitor visitor used to convert subqueries/relations
   * @param callConverters converters for Rex calls
   */
  public RexExpressionConverter(SubstraitRelVisitor relVisitor, CallConverter... callConverters) {
    this(relVisitor, Arrays.asList(callConverters), null, TypeConverter.DEFAULT);
  }

  /**
   * Creates a converter with the given call converters and default {@link TypeConverter}.
   *
   * @param callConverters converters for Rex calls
   */
  public RexExpressionConverter(CallConverter... callConverters) {
    this(null, Arrays.asList(callConverters), null, TypeConverter.DEFAULT);
  }

  /**
   * Creates a converter with full configuration.
   *
   * @param relVisitor visitor used to convert subqueries/relations; may be {@code null}
   * @param callConverters converters for Rex calls
   * @param windowFunctionConverter converter for window functions; may be {@code null}
   * @param typeConverter converter from Calcite types to Substrait types
   */
  public RexExpressionConverter(
      SubstraitRelVisitor relVisitor,
      List<CallConverter> callConverters,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter) {
    this.callConverters = callConverters;
    this.relVisitor = relVisitor;
    this.windowFunctionConverter = windowFunctionConverter;
    this.typeConverter = typeConverter;
  }

  /**
   * Testing-only constructor that wires default converters.
   *
   * <p>Missing {@code ScalarFunctionConverter} and {@code CallConverters.CREATE_SEARCH_CONV}.
   */
  public RexExpressionConverter() {
    this(null, CallConverters.defaults(TypeConverter.DEFAULT), null, TypeConverter.DEFAULT);
    // TODO: Hide this AND/OR UPDATE tests
  }

  /**
   * Converts a {@link RexInputRef} to a root struct field reference.
   *
   * @param inputRef the input reference
   * @return a Substrait field reference expression
   */
  @Override
  public Expression visitInputRef(RexInputRef inputRef) {
    return FieldReference.newRootStructReference(
        inputRef.getIndex(), typeConverter.toSubstrait(inputRef.getType()));
  }

  /**
   * Converts a {@link RexCall} using registered {@link CallConverter}s.
   *
   * @param call the Rex call node
   * @return the converted Substrait expression
   * @throws IllegalArgumentException if no converter can handle the call
   */
  @Override
  public Expression visitCall(RexCall call) {
    for (CallConverter c : callConverters) {
      Optional<Expression> out = c.convert(call, rexNode -> rexNode.accept(this));
      if (out.isPresent()) {
        return out.get();
      }
    }

    throw new IllegalArgumentException(callConversionFailureMessage(call));
  }

  /**
   * Builds a concise failure message for an unsupported call conversion.
   *
   * @param call the Rex call node
   * @return a human-readable message describing the failure
   */
  private String callConversionFailureMessage(RexCall call) {
    return String.format(
        "Unable to convert call %s(%s).",
        call.getOperator().getName(),
        call.getOperands().stream()
            .map(t -> t.accept(this).getType().accept(new StringTypeVisitor()))
            .collect(Collectors.joining(", ")));
  }

  /**
   * Converts a {@link RexLiteral} to a Substrait literal expression.
   *
   * @param literal the Rex literal
   * @return the converted Substrait expression
   */
  @Override
  public Expression visitLiteral(RexLiteral literal) {
    return (new LiteralConverter(typeConverter)).convert(literal);
  }

  /**
   * Converts a {@link RexOver} window function call.
   *
   * @param over the windowed call
   * @return the converted Substrait expression
   * @throws IllegalArgumentException if {@code IGNORE NULLS} is used or conversion fails
   */
  @Override
  public Expression visitOver(RexOver over) {
    if (over.ignoreNulls()) {
      throw new IllegalArgumentException("IGNORE NULLS cannot be expressed in Substrait");
    }

    return windowFunctionConverter
        .convert(over, rexNode -> rexNode.accept(this), this)
        .orElseThrow(() -> new IllegalArgumentException(callConversionFailureMessage(over)));
  }

  /**
   * Not supported.
   *
   * @param correlVariable the correl variable
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new UnsupportedOperationException("RexCorrelVariable not supported");
  }

  /**
   * Not supported.
   *
   * @param dynamicParam the dynamic parameter
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new UnsupportedOperationException("RexDynamicParam not supported");
  }

  /**
   * Not supported.
   *
   * @param rangeRef the range ref
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitRangeRef(RexRangeRef rangeRef) {
    throw new UnsupportedOperationException("RexRangeRef not supported");
  }

  /**
   * Converts a {@link RexFieldAccess} to a Substrait field reference expression.
   *
   * @param fieldAccess the field access
   * @return the converted Substrait expression
   * @throws UnsupportedOperationException for unsupported reference kinds
   */
  @Override
  public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
    SqlKind kind = fieldAccess.getReferenceExpr().getKind();
    switch (kind) {
      case CORREL_VARIABLE:
        {
          int stepsOut = relVisitor.getFieldAccessDepth(fieldAccess);

          return FieldReference.newRootStructOuterReference(
              fieldAccess.getField().getIndex(),
              typeConverter.toSubstrait(fieldAccess.getType()),
              stepsOut);
        }
      case ITEM:
      case INPUT_REF:
      case FIELD_ACCESS:
        {
          Expression expression = fieldAccess.getReferenceExpr().accept(this);
          if (expression instanceof FieldReference) {
            FieldReference nestedReference = (FieldReference) expression;
            return nestedReference.dereferenceStruct(fieldAccess.getField().getIndex());
          } else {
            return FieldReference.newStructReference(fieldAccess.getField().getIndex(), expression);
          }
        }
      default:
        throw new UnsupportedOperationException(
            String.format("RexFieldAccess for SqlKind %s not supported", kind));
    }
  }

  /**
   * Converts a {@link RexSubQuery} into a Substrait set or scalar subquery expression.
   *
   * @param subQuery the subquery node
   * @return the converted Substrait expression
   * @throws UnsupportedOperationException for unsupported subquery operators
   */
  @Override
  public Expression visitSubQuery(RexSubQuery subQuery) {
    Rel rel = relVisitor.apply(subQuery.rel);

    if (subQuery.getOperator() == SqlStdOperatorTable.EXISTS) {
      return Expression.SetPredicate.builder()
          .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
          .tuples(rel)
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.UNIQUE) {
      return Expression.SetPredicate.builder()
          .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNIQUE)
          .tuples(rel)
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.SCALAR_QUERY) {
      return Expression.ScalarSubquery.builder()
          .input(rel)
          .type(typeConverter.toSubstrait(subQuery.getType()))
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.IN) {
      List<Expression> needles = new ArrayList<>();
      for (RexNode inOperand : subQuery.getOperands()) {
        needles.add(inOperand.accept(this));
      }
      return Expression.InPredicate.builder().needles(needles).haystack(rel).build();
    }

    throw new UnsupportedOperationException("RexSubQuery not supported");
  }

  /**
   * Not supported.
   *
   * @param fieldRef the table input reference
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException("RexTableInputRef not supported");
  }

  /**
   * Not supported.
   *
   * @param localRef the local reference
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException("RexLocalRef not supported");
  }

  /**
   * Not supported.
   *
   * @param fieldRef the pattern field reference
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new UnsupportedOperationException("RexPatternFieldRef not supported");
  }

  /**
   * Not supported.
   *
   * @param rexLambda the lambda
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitLambda(RexLambda rexLambda) {
    throw new UnsupportedOperationException("RexLambda not supported");
  }

  /**
   * Not supported.
   *
   * @param rexLambdaRef the lambda reference
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitLambdaRef(RexLambdaRef rexLambdaRef) {
    throw new UnsupportedOperationException("RexLambdaRef not supported");
  }

  /**
   * Not supported.
   *
   * @param nodeAndFieldIndex the node/field index wrapper
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public Expression visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
    throw new UnsupportedOperationException("RexNodeAndFieldIndex not supported");
  }
}
