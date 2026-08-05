package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.PrecisionTimeLiteral;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A copy-on-write visitor for expressions that creates modified copies only when changes are made.
 *
 * <p>This visitor traverses expression trees and returns Optional.empty() when no changes are
 * needed, or Optional containing a modified expression when changes are made.
 *
 * @param <E> the exception type that may be thrown during visitation
 */
public class ExpressionCopyOnWriteVisitor<E extends Exception>
    implements ExpressionVisitor<Optional<Expression>, EmptyVisitationContext, E> {

  private final RelCopyOnWriteVisitor<E> relCopyOnWriteVisitor;

  /**
   * Constructs an ExpressionCopyOnWriteVisitor.
   *
   * @param relCopyOnWriteVisitor the relation visitor to use for visiting relation nodes
   */
  public ExpressionCopyOnWriteVisitor(RelCopyOnWriteVisitor<E> relCopyOnWriteVisitor) {
    this.relCopyOnWriteVisitor = relCopyOnWriteVisitor;
  }

  /**
   * Gets the relation copy-on-write visitor.
   *
   * @return the RelCopyOnWriteVisitor instance
   */
  protected final RelCopyOnWriteVisitor<E> getRelCopyOnWriteVisitor() {
    return this.relCopyOnWriteVisitor;
  }

  /**
   * Utility method for visiting literals. By default, visits to literal types call this.
   *
   * @param literal the literal expression to visit
   * @return Optional.empty() as literals are not modified by default
   */
  public Optional<Expression> visitLiteral(Expression.Literal literal) {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(Expression.NullLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.BoolLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I8Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I16Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I32Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I64Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FP32Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FP64Literal expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.StrLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.BinaryLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(PrecisionTimeLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.DateLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.PrecisionTimestampLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.PrecisionTimestampTZLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.IntervalYearLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.IntervalDayLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.IntervalCompoundLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.UUIDLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.FixedCharLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.VarCharLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.FixedBinaryLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.DecimalLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.MapLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.EmptyMapLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.ListLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.EmptyListLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.StructLiteral expr, EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.NestedStruct expr, EmptyVisitationContext context)
      throws E {
    Optional<List<Expression>> expressions = visitExprList(expr.fields(), context);
    return expressions.map(
        expressionList ->
            Expression.NestedStruct.builder().from(expr).fields(expressionList).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.UserDefinedAnyLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.UserDefinedStructLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.Switch expr, EmptyVisitationContext context)
      throws E {
    Optional<Expression> match = expr.match().accept(this, context);
    Optional<List<Expression.SwitchClause>> switchClauses =
        transformList(expr.switchClauses(), context, this::visitSwitchClause);
    Optional<Expression> defaultClause = expr.defaultClause().accept(this, context);

    if (allEmpty(match, switchClauses, defaultClause)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.Switch.builder()
            .from(expr)
            .match(match.orElse(expr.match()))
            .switchClauses(switchClauses.orElse(expr.switchClauses()))
            .defaultClause(defaultClause.orElse(expr.defaultClause()))
            .build());
  }

  /**
   * Visits a switch clause.
   *
   * @param switchClause the switch clause to visit
   * @param context the visitation context
   * @return Optional containing modified switch clause, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<Expression.SwitchClause> visitSwitchClause(
      Expression.SwitchClause switchClause, EmptyVisitationContext context) throws E {
    // This code does not visit the condition on the switch clause as that MUST be a Literal and the
    // visitor does not guarantee a Literal return type. If you wish to update the condition,
    // override this method.
    return switchClause
        .then()
        .accept(this, context)
        .map(then -> Expression.SwitchClause.builder().from(switchClause).then(then).build());
  }

  @Override
  public Optional<Expression> visit(Expression.IfThen ifThen, EmptyVisitationContext context)
      throws E {
    Optional<List<Expression.IfClause>> ifClauses =
        transformList(ifThen.ifClauses(), context, this::visitIfClause);
    Optional<Expression> elseClause = ifThen.elseClause().accept(this, context);

    if (allEmpty(ifClauses, elseClause)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.IfThen.builder()
            .from(ifThen)
            .ifClauses(ifClauses.orElse(ifThen.ifClauses()))
            .elseClause(elseClause.orElse(ifThen.elseClause()))
            .build());
  }

  /**
   * Visits an if clause.
   *
   * @param ifClause the if clause to visit
   * @param context the visitation context
   * @return Optional containing modified if clause, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<Expression.IfClause> visitIfClause(
      Expression.IfClause ifClause, EmptyVisitationContext context) throws E {
    Optional<Expression> condition = ifClause.condition().accept(this, context);
    Optional<Expression> then = ifClause.then().accept(this, context);

    if (allEmpty(condition, then)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.IfClause.builder()
            .from(ifClause)
            .condition(condition.orElse(ifClause.condition()))
            .then(then.orElse(ifClause.then()))
            .build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.ScalarFunctionInvocation sfi, EmptyVisitationContext context) throws E {
    return visitFunctionArguments(sfi.arguments(), context)
        .map(
            arguments ->
                Expression.ScalarFunctionInvocation.builder()
                    .from(sfi)
                    .arguments(arguments)
                    .build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.WindowFunctionInvocation wfi, EmptyVisitationContext context) throws E {
    Optional<List<FunctionArg>> arguments = visitFunctionArguments(wfi.arguments(), context);
    Optional<List<Expression>> partitionBy = visitExprList(wfi.partitionBy(), context);
    Optional<List<Expression.SortField>> sort =
        transformList(wfi.sort(), context, this::visitSortField);

    if (allEmpty(arguments, partitionBy, sort)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.WindowFunctionInvocation.builder()
            .from(wfi)
            .arguments(arguments.orElse(wfi.arguments()))
            .partitionBy(partitionBy.orElse(wfi.partitionBy()))
            .sort(sort.orElse(wfi.sort()))
            .build());
  }

  @Override
  public Optional<Expression> visit(Expression.Cast cast, EmptyVisitationContext context) throws E {
    return cast.input()
        .accept(this, context)
        .map(input -> Expression.Cast.builder().from(cast).input(input).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.SingleOrList singleOrList, EmptyVisitationContext context) throws E {
    Optional<Expression> condition = singleOrList.condition().accept(this, context);
    Optional<List<Expression>> options = visitExprList(singleOrList.options(), context);

    if (allEmpty(condition, options)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.SingleOrList.builder()
            .from(singleOrList)
            .condition(condition.orElse(singleOrList.condition()))
            .options(options.orElse(singleOrList.options()))
            .build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.MultiOrList multiOrList, EmptyVisitationContext context) throws E {
    Optional<List<Expression>> conditions = visitExprList(multiOrList.conditions(), context);
    Optional<List<Expression.MultiOrListRecord>> optionCombinations =
        transformList(multiOrList.optionCombinations(), context, this::visitMultiOrListRecord);

    if (allEmpty(conditions, optionCombinations)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.MultiOrList.builder()
            .from(multiOrList)
            .conditions(conditions.orElse(multiOrList.conditions()))
            .optionCombinations(optionCombinations.orElse(multiOrList.optionCombinations()))
            .build());
  }

  @Override
  public Optional<Expression> visit(Expression.NestedList expr, EmptyVisitationContext context)
      throws E {
    Optional<List<Expression>> expressions = visitExprList(expr.values(), context);

    return expressions.map(
        expressionList ->
            Expression.NestedList.builder().from(expr).values(expressionList).build());
  }

  /**
   * Visits a multi-or-list record.
   *
   * @param multiOrListRecord the multi-or-list record to visit
   * @param context the visitation context
   * @return Optional containing modified record, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<Expression.MultiOrListRecord> visitMultiOrListRecord(
      Expression.MultiOrListRecord multiOrListRecord, EmptyVisitationContext context) throws E {
    return visitExprList(multiOrListRecord.values(), context)
        .map(
            values ->
                Expression.MultiOrListRecord.builder()
                    .from(multiOrListRecord)
                    .values(values)
                    .build());
  }

  @Override
  public Optional<Expression> visit(FieldReference fieldReference, EmptyVisitationContext context)
      throws E {
    return visitFieldReference(fieldReference, context).map(Expression.class::cast);
  }

  /**
   * Visits a field reference, rewriting the expression it is rooted at, if any, and re-deriving its
   * cached type from the root it resolves against. Re-deriving the type is what keeps references
   * correct when a relation's input is replaced by one emitting a different record type.
   *
   * <p>The type of a reference to a lambda parameter, or of an outer reference identified by the
   * {@link io.substrait.relation.Rel#getRelAnchor() rel anchor} of the relation it is rooted on, is
   * left as it is: neither resolves against a relation in the enclosing scopes tracked during the
   * traversal.
   *
   * @param fieldReference the field reference to visit
   * @param context the visitation context
   * @return Optional containing the modified field reference, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  public Optional<FieldReference> visitFieldReference(
      FieldReference fieldReference, EmptyVisitationContext context) throws E {
    if (fieldReference.inputExpression().isPresent()) {
      Optional<Expression> inputExpression =
          fieldReference.inputExpression().get().accept(this, context);
      if (!inputExpression.isPresent()) {
        return Optional.empty();
      }
      ImmutableFieldReference.Builder rewritten =
          ImmutableFieldReference.builder().from(fieldReference).inputExpression(inputExpression);
      if (!fieldReference.segments().isEmpty()) {
        rewritten.type(
            FieldReference.ofExpression(inputExpression.get(), segmentsOf(fieldReference)).type());
      }
      return Optional.of(rewritten.build());
    }
    return retypeRootReference(fieldReference);
  }

  /**
   * Re-derives the type of a reference into the record type of the relation it is rooted on, which
   * may have been replaced by one emitting a different record type.
   */
  private Optional<FieldReference> retypeRootReference(FieldReference fieldReference) {
    if (fieldReference.isLambdaParameterReference()
        || fieldReference.outerReferenceRelReference().isPresent()) {
      return Optional.empty();
    }
    Type.Struct rootType =
        getRelCopyOnWriteVisitor()
            .inputTypeStepsOut(fieldReference.outerReferenceStepsOut().orElse(0));
    // A rewrite that drops fields can leave a reference selecting a field its input no longer has.
    // The resulting relation tree is invalid either way, so leave the reference as it is rather
    // than turn re-deriving its type into a failure of the rewrite.
    if (rootType == null || !selectsFieldOf(fieldReference, rootType)) {
      return Optional.empty();
    }
    Type type = FieldReference.ofRoot(rootType, segmentsOf(fieldReference)).type();
    if (type.equals(fieldReference.type())) {
      return Optional.empty();
    }
    return Optional.of(ImmutableFieldReference.builder().from(fieldReference).type(type).build());
  }

  /**
   * Returns whether the outermost segment of the given reference selects a field that the given
   * record type has. The segments are held innermost first, so the outermost one is the last.
   */
  private static boolean selectsFieldOf(FieldReference fieldReference, Type.Struct rootType) {
    List<FieldReference.ReferenceSegment> segments = fieldReference.segments();
    if (segments.isEmpty()) {
      return false;
    }
    FieldReference.ReferenceSegment outermost = segments.get(segments.size() - 1);
    return outermost instanceof FieldReference.StructField
        && ((FieldReference.StructField) outermost).offset() < rootType.fields().size();
  }

  /**
   * Returns the segments of the given reference as a mutable list, which is what {@link
   * FieldReference#ofRoot} and {@link FieldReference#ofExpression} require: they reverse the list
   * they are given.
   */
  private static List<FieldReference.ReferenceSegment> segmentsOf(FieldReference fieldReference) {
    return new ArrayList<>(fieldReference.segments());
  }

  @Override
  public Optional<Expression> visit(
      Expression.SetPredicate setPredicate, EmptyVisitationContext context) throws E {
    return getRelCopyOnWriteVisitor()
        .inSubqueryScope(() -> setPredicate.tuples().accept(getRelCopyOnWriteVisitor(), context))
        .map(tuple -> Expression.SetPredicate.builder().from(setPredicate).tuples(tuple).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.ScalarSubquery scalarSubquery, EmptyVisitationContext context) throws E {
    return getRelCopyOnWriteVisitor()
        .inSubqueryScope(() -> scalarSubquery.input().accept(getRelCopyOnWriteVisitor(), context))
        .map(
            input -> Expression.ScalarSubquery.builder().from(scalarSubquery).input(input).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.InPredicate inPredicate, EmptyVisitationContext context) throws E {
    // The needles are evaluated in the current scope; only the haystack is a subquery boundary.
    Optional<List<Expression>> needles = visitExprList(inPredicate.needles(), context);
    Optional<Rel> haystack =
        getRelCopyOnWriteVisitor()
            .inSubqueryScope(
                () -> inPredicate.haystack().accept(getRelCopyOnWriteVisitor(), context));

    if (allEmpty(haystack, needles)) {
      return Optional.empty();
    }
    return Optional.of(
        Expression.InPredicate.builder()
            .from(inPredicate)
            .haystack(haystack.orElse(inPredicate.haystack()))
            .needles(needles.orElse(inPredicate.needles()))
            .build());
  }

  @Override
  public Optional<Expression> visit(Expression.Lambda lambda, EmptyVisitationContext context)
      throws E {
    Optional<Expression> newBody = lambda.body().accept(this, context);

    if (allEmpty(newBody)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableExpression.Lambda.builder()
            .from(lambda)
            .body(newBody.orElse(lambda.body()))
            .build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.DynamicParameter expr, EmptyVisitationContext context) throws E {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(
      Expression.CurrentTimestamp expr, EmptyVisitationContext context) throws E {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(Expression.CurrentTimezone expr, EmptyVisitationContext context)
      throws E {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(Expression.CurrentDate expr, EmptyVisitationContext context)
      throws E {
    return Optional.empty();
  }

  // utilities

  /**
   * Visits a list of expressions.
   *
   * @param exprs the list of expressions to visit
   * @param context the visitation context
   * @return Optional containing modified list, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<List<Expression>> visitExprList(
      List<Expression> exprs, EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (e, c) -> e.accept(this, c));
  }

  /**
   * Visits a list of function arguments.
   *
   * @param funcArgs the list of function arguments to visit
   * @param context the visitation context
   * @return Optional containing modified list, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<List<FunctionArg>> visitFunctionArguments(
      List<FunctionArg> funcArgs, EmptyVisitationContext context) throws E {
    return CopyOnWriteUtils.<FunctionArg, EmptyVisitationContext, E>transformList(
        funcArgs,
        context,
        (arg, c) -> {
          if (arg instanceof Expression) {
            return ((Expression) arg).accept(this, c).flatMap(Optional::<FunctionArg>of);
          } else {
            return Optional.empty();
          }
        });
  }

  /**
   * Visits a sort field.
   *
   * @param sortField the sort field to visit
   * @param context the visitation context
   * @return Optional containing modified sort field, or empty if no changes
   * @throws E if an error occurs during visitation
   */
  protected Optional<Expression.SortField> visitSortField(
      Expression.SortField sortField, EmptyVisitationContext context) throws E {
    return sortField
        .expr()
        .accept(this, context)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }
}
