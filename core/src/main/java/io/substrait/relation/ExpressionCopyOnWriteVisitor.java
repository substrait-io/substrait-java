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

  public ExpressionCopyOnWriteVisitor(final RelCopyOnWriteVisitor<E> relCopyOnWriteVisitor) {
    this.relCopyOnWriteVisitor = relCopyOnWriteVisitor;
  }

  protected final RelCopyOnWriteVisitor<E> getRelCopyOnWriteVisitor() {
    return this.relCopyOnWriteVisitor;
  }

  /** Utility method for visiting literals. By default, visits to literal types call this. */
  public Optional<Expression> visitLiteral(final Expression.Literal literal) {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(
      final Expression.NullLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.BoolLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.I8Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.I16Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.I32Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.I64Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.FP32Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.FP64Literal expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.StrLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.BinaryLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.TimeLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.DateLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.TimestampLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.TimestampTZLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.PrecisionTimestampLiteral expr, final EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.PrecisionTimestampTZLiteral expr, final EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.IntervalYearLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.IntervalDayLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.IntervalCompoundLiteral expr, final EmptyVisitationContext context)
      throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.UUIDLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.FixedCharLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.VarCharLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.FixedBinaryLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.DecimalLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.MapLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.EmptyMapLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.ListLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.EmptyListLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.StructLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.UserDefinedLiteral expr, final EmptyVisitationContext context) throws E {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(
      final Expression.Switch expr, final EmptyVisitationContext context) throws E {
    final Optional<Expression> match = expr.match().accept(this, context);
    final Optional<List<Expression.SwitchClause>> switchClauses =
        transformList(expr.switchClauses(), context, this::visitSwitchClause);
    final Optional<Expression> defaultClause = expr.defaultClause().accept(this, context);

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
      final Expression.SwitchClause switchClause, final EmptyVisitationContext context) throws E {
    // This code does not visit the condition on the switch clause as that MUST be a Literal and the
    // visitor does not guarantee a Literal return type. If you wish to update the condition,
    // override this method.
    return switchClause
        .then()
        .accept(this, context)
        .map(then -> Expression.SwitchClause.builder().from(switchClause).then(then).build());
  }

  @Override
  public Optional<Expression> visit(
      final Expression.IfThen ifThen, final EmptyVisitationContext context) throws E {
    final Optional<List<Expression.IfClause>> ifClauses =
        transformList(ifThen.ifClauses(), context, this::visitIfClause);
    final Optional<Expression> elseClause = ifThen.elseClause().accept(this, context);

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
      final Expression.IfClause ifClause, final EmptyVisitationContext context) throws E {
    final Optional<Expression> condition = ifClause.condition().accept(this, context);
    final Optional<Expression> then = ifClause.then().accept(this, context);

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
      final Expression.ScalarFunctionInvocation sfi, final EmptyVisitationContext context)
      throws E {
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
      final Expression.WindowFunctionInvocation wfi, final EmptyVisitationContext context)
      throws E {
    final Optional<List<FunctionArg>> arguments = visitFunctionArguments(wfi.arguments(), context);
    final Optional<List<Expression>> partitionBy = visitExprList(wfi.partitionBy(), context);
    final Optional<List<Expression.SortField>> sort =
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
  public Optional<Expression> visit(
      final Expression.Cast cast, final EmptyVisitationContext context) throws E {
    return cast.input()
        .accept(this, context)
        .map(input -> Expression.Cast.builder().from(cast).input(input).build());
  }

  @Override
  public Optional<Expression> visit(
      final Expression.SingleOrList singleOrList, final EmptyVisitationContext context) throws E {
    final Optional<Expression> condition = singleOrList.condition().accept(this, context);
    final Optional<List<Expression>> options = visitExprList(singleOrList.options(), context);

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
      final Expression.MultiOrList multiOrList, final EmptyVisitationContext context) throws E {
    final Optional<List<Expression>> conditions = visitExprList(multiOrList.conditions(), context);
    final Optional<List<Expression.MultiOrListRecord>> optionCombinations =
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

  protected Optional<Expression.MultiOrListRecord> visitMultiOrListRecord(
      final Expression.MultiOrListRecord multiOrListRecord, final EmptyVisitationContext context)
      throws E {
    return visitExprList(multiOrListRecord.values(), context)
        .map(
            values ->
                Expression.MultiOrListRecord.builder()
                    .from(multiOrListRecord)
                    .values(values)
                    .build());
  }

  @Override
  public Optional<Expression> visit(
      final FieldReference fieldReference, final EmptyVisitationContext context) throws E {
    final Optional<Expression> inputExpression =
        visitOptionalExpression(fieldReference.inputExpression(), context);

    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }
    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  @Override
  public Optional<Expression> visit(
      final Expression.SetPredicate setPredicate, final EmptyVisitationContext context) throws E {
    return setPredicate
        .tuples()
        .accept(getRelCopyOnWriteVisitor(), context)
        .map(tuple -> Expression.SetPredicate.builder().from(setPredicate).tuples(tuple).build());
  }

  @Override
  public Optional<Expression> visit(
      final Expression.ScalarSubquery scalarSubquery, final EmptyVisitationContext context)
      throws E {
    return scalarSubquery
        .input()
        .accept(getRelCopyOnWriteVisitor(), context)
        .map(
            input -> Expression.ScalarSubquery.builder().from(scalarSubquery).input(input).build());
  }

  @Override
  public Optional<Expression> visit(
      final Expression.InPredicate inPredicate, final EmptyVisitationContext context) throws E {
    final Optional<Rel> haystack =
        inPredicate.haystack().accept(getRelCopyOnWriteVisitor(), context);
    final Optional<List<Expression>> needles = visitExprList(inPredicate.needles(), context);

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
      final List<Expression> exprs, final EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (e, c) -> e.accept(this, c));
  }

  private Optional<Expression> visitOptionalExpression(
      final Optional<Expression> optExpr, final EmptyVisitationContext context) throws E {
    // not using optExpr.map to allow us to propagate the EXCEPTION nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(this, context);
    }
    return Optional.empty();
  }

  protected Optional<List<FunctionArg>> visitFunctionArguments(
      final List<FunctionArg> funcArgs, final EmptyVisitationContext context) throws E {
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
      final Expression.SortField sortField, final EmptyVisitationContext context) throws E {
    return sortField
        .expr()
        .accept(this, context)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }
}
