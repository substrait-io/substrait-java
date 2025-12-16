package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.Optional;

public class ExpressionCopyOnWriteVisitor<E extends Exception>
    implements ExpressionVisitor<Optional<Expression>, EmptyVisitationContext, E> {

  private final RelCopyOnWriteVisitor<E> relCopyOnWriteVisitor;

  public ExpressionCopyOnWriteVisitor(RelCopyOnWriteVisitor<E> relCopyOnWriteVisitor) {
    this.relCopyOnWriteVisitor = relCopyOnWriteVisitor;
  }

  protected final RelCopyOnWriteVisitor<E> getRelCopyOnWriteVisitor() {
    return this.relCopyOnWriteVisitor;
  }

  /** Utility method for visiting literals. By default, visits to literal types call this. */
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
  public Optional<Expression> visit(Expression.TimeLiteral expr, EmptyVisitationContext context)
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
      Expression.TimestampLiteral expr, EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      Expression.TimestampTZLiteral expr, EmptyVisitationContext context) throws E {
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
    Optional<Expression> inputExpression =
        visitOptionalExpression(fieldReference.inputExpression(), context);

    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }
    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.SetPredicate setPredicate, EmptyVisitationContext context) throws E {
    return setPredicate
        .tuples()
        .accept(getRelCopyOnWriteVisitor(), context)
        .map(tuple -> Expression.SetPredicate.builder().from(setPredicate).tuples(tuple).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.ScalarSubquery scalarSubquery, EmptyVisitationContext context) throws E {
    return scalarSubquery
        .input()
        .accept(getRelCopyOnWriteVisitor(), context)
        .map(
            input -> Expression.ScalarSubquery.builder().from(scalarSubquery).input(input).build());
  }

  @Override
  public Optional<Expression> visit(
      Expression.InPredicate inPredicate, EmptyVisitationContext context) throws E {
    Optional<Rel> haystack = inPredicate.haystack().accept(getRelCopyOnWriteVisitor(), context);
    Optional<List<Expression>> needles = visitExprList(inPredicate.needles(), context);

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

  // utilities

  protected Optional<List<Expression>> visitExprList(
      List<Expression> exprs, EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (e, c) -> e.accept(this, c));
  }

  private Optional<Expression> visitOptionalExpression(
      Optional<Expression> optExpr, EmptyVisitationContext context) throws E {
    // not using optExpr.map to allow us to propagate the EXCEPTION nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(this, context);
    }
    return Optional.empty();
  }

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

  protected Optional<Expression.SortField> visitSortField(
      Expression.SortField sortField, EmptyVisitationContext context) throws E {
    return sortField
        .expr()
        .accept(this, context)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }
}
