package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import java.util.List;
import java.util.Optional;

public class ExpressionCopyOnWriteVisitor<EXCEPTION extends Exception>
    implements ExpressionVisitor<Optional<Expression>, EXCEPTION> {

  private final RelCopyOnWriteVisitor<EXCEPTION> relCopyOnWriteVisitor;

  public ExpressionCopyOnWriteVisitor(RelCopyOnWriteVisitor<EXCEPTION> relCopyOnWriteVisitor) {
    this.relCopyOnWriteVisitor = relCopyOnWriteVisitor;
  }

  protected final RelCopyOnWriteVisitor<EXCEPTION> getRelCopyOnWriteVisitor() {
    return this.relCopyOnWriteVisitor;
  }

  /** Utility method for visiting literals. By default, visits to literal types call this. */
  public Optional<Expression> visitLiteral(Expression.Literal literal) {
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visit(Expression.NullLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.BoolLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I8Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I16Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I32Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.I64Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FP32Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FP64Literal expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.StrLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.BinaryLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.TimeLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.DateLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.TimestampLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.TimestampTZLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.PrecisionTimestampLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.PrecisionTimestampTZLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.IntervalYearLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.IntervalDayLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.IntervalCompoundLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.UUIDLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FixedCharLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.VarCharLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.FixedBinaryLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.DecimalLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.MapLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.ListLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.EmptyListLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.StructLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.UserDefinedLiteral expr) throws EXCEPTION {
    return visitLiteral(expr);
  }

  @Override
  public Optional<Expression> visit(Expression.Switch expr) throws EXCEPTION {
    var match = expr.match().accept(this);
    var switchClauses = transformList(expr.switchClauses(), this::visitSwitchClause);
    var defaultClause = expr.defaultClause().accept(this);

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
      Expression.SwitchClause switchClause) throws EXCEPTION {
    // This code does not visit the condition on the switch clause as that MUST be a Literal and the
    // visitor does not guarantee a Literal return type. If you wish to update the condition,
    // override this method.
    return switchClause
        .then()
        .accept(this)
        .map(then -> Expression.SwitchClause.builder().from(switchClause).then(then).build());
  }

  @Override
  public Optional<Expression> visit(Expression.IfThen ifThen) throws EXCEPTION {
    var ifClauses = transformList(ifThen.ifClauses(), this::visitIfClause);
    var elseClause = ifThen.elseClause().accept(this);

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

  protected Optional<Expression.IfClause> visitIfClause(Expression.IfClause ifClause)
      throws EXCEPTION {
    var condition = ifClause.condition().accept(this);
    var then = ifClause.then().accept(this);

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
  public Optional<Expression> visit(Expression.ScalarFunctionInvocation sfi) throws EXCEPTION {
    return visitFunctionArguments(sfi.arguments())
        .map(
            arguments ->
                Expression.ScalarFunctionInvocation.builder()
                    .from(sfi)
                    .arguments(arguments)
                    .build());
  }

  @Override
  public Optional<Expression> visit(Expression.WindowFunctionInvocation wfi) throws EXCEPTION {
    var arguments = visitFunctionArguments(wfi.arguments());
    var partitionBy = visitExprList(wfi.partitionBy());
    var sort = transformList(wfi.sort(), this::visitSortField);

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
  public Optional<Expression> visit(Expression.Cast cast) throws EXCEPTION {
    return cast.input()
        .accept(this)
        .map(input -> Expression.Cast.builder().from(cast).input(input).build());
  }

  @Override
  public Optional<Expression> visit(Expression.SingleOrList singleOrList) throws EXCEPTION {
    var condition = singleOrList.condition().accept(this);
    var options = visitExprList(singleOrList.options());

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
  public Optional<Expression> visit(Expression.MultiOrList multiOrList) throws EXCEPTION {
    var conditions = visitExprList(multiOrList.conditions());
    var optionCombinations =
        transformList(multiOrList.optionCombinations(), this::visitMultiOrListRecord);

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
      Expression.MultiOrListRecord multiOrListRecord) throws EXCEPTION {
    return visitExprList(multiOrListRecord.values())
        .map(
            values ->
                Expression.MultiOrListRecord.builder()
                    .from(multiOrListRecord)
                    .values(values)
                    .build());
  }

  @Override
  public Optional<Expression> visit(FieldReference fieldReference) throws EXCEPTION {
    var inputExpression = visitOptionalExpression(fieldReference.inputExpression());

    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }
    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  @Override
  public Optional<Expression> visit(Expression.SetPredicate setPredicate) throws EXCEPTION {
    return setPredicate
        .tuples()
        .accept(getRelCopyOnWriteVisitor())
        .map(tuple -> Expression.SetPredicate.builder().from(setPredicate).tuples(tuple).build());
  }

  @Override
  public Optional<Expression> visit(Expression.ScalarSubquery scalarSubquery) throws EXCEPTION {
    return scalarSubquery
        .input()
        .accept(getRelCopyOnWriteVisitor())
        .map(
            input -> Expression.ScalarSubquery.builder().from(scalarSubquery).input(input).build());
  }

  @Override
  public Optional<Expression> visit(Expression.InPredicate inPredicate) throws EXCEPTION {
    var haystack = inPredicate.haystack().accept(getRelCopyOnWriteVisitor());
    var needles = visitExprList(inPredicate.needles());

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

  protected Optional<List<Expression>> visitExprList(List<Expression> exprs) throws EXCEPTION {
    return transformList(exprs, e -> e.accept(this));
  }

  private Optional<Expression> visitOptionalExpression(Optional<Expression> optExpr)
      throws EXCEPTION {
    // not using optExpr.map to allow us to propagate the EXCEPTION nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(this);
    }
    return Optional.empty();
  }

  protected Optional<List<FunctionArg>> visitFunctionArguments(List<FunctionArg> funcArgs)
      throws EXCEPTION {
    return CopyOnWriteUtils.<FunctionArg, EXCEPTION>transformList(
        funcArgs,
        arg -> {
          if (arg instanceof Expression expr) {
            return expr.accept(this).flatMap(Optional::<FunctionArg>of);
          } else {
            return Optional.empty();
          }
        });
  }

  protected Optional<Expression.SortField> visitSortField(Expression.SortField sortField)
      throws EXCEPTION {
    return sortField
        .expr()
        .accept(this)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }
}
