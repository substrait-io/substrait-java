package io.substrait.relation;

import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Class used to visit all child relations from a root relation and optionally replace subtrees by
 * overriding a visitor method. The traversal will include relations inside of subquery expressions.
 * By default, no subtree substitution will be performed. However, if a visit method is overridden
 * to return a non-empty optional value, then that value will replace the relation in the tree.
 */
public class RelCopyOnWriteVisitor extends AbstractRelVisitor<Optional<Rel>, RuntimeException> {

  public static <T> Optional<List<T>> transformList(
      List<T> items, Function<T, Optional<T>> transform) {
    List<T> transformedItems = items;
    for (int i = 0; i < items.size(); i++) {
      var item = items.get(i);
      var transformedItem = transform.apply(item);
      if (transformedItem.isPresent()) {
        if (transformedItems == items) {
          transformedItems = new ArrayList<>(items);
        }
        transformedItems.set(i, transformedItem.get());
      }
    }
    return transformedItems == items ? Optional.empty() : Optional.of(transformedItems);
  }

  private Optional<List<Expression>> transformExpressions(List<Expression> oldExpressions) {
    return transformList(oldExpressions, t -> this.visitExpression(t));
  }

  private Optional<List<FunctionArg>> transformFuncArgs(List<FunctionArg> oldExpressions) {
    return transformList(
        oldExpressions,
        t -> {
          if (t instanceof Expression) {
            return this.visitExpression((Expression) t).flatMap(ex -> Optional.<FunctionArg>of(ex));
          }
          return Optional.of(t);
        });
  }

  private static boolean allEmpty(Optional<?>... optionals) {
    for (Optional<?> optional : optionals) {
      if (optional.isPresent()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Optional<Rel> visitFallback(Rel rel) {
    return Optional.empty();
  }

  @Override
  public Optional<Rel> visit(Aggregate aggregate) throws RuntimeException {
    return aggregate
        .getInput()
        .accept(this)
        .map(input -> ImmutableAggregate.builder().from(aggregate).input(input).build());
  }

  @Override
  public Optional<Rel> visit(Fetch fetch) throws RuntimeException {
    return fetch
        .getInput()
        .accept(this)
        .map(input -> ImmutableFetch.builder().from(fetch).input(input).build());
  }

  @Override
  public Optional<Rel> visit(Filter filter) throws RuntimeException {
    var input = filter.getInput().accept(this);
    var condition = visitExpression(filter.getCondition());
    if (allEmpty(input, condition)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableFilter.builder()
            .from(filter)
            .input(input.orElse(filter.getInput()))
            .condition(condition.orElse(filter.getCondition()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Join join) throws RuntimeException {
    var left = join.getLeft().accept(this);
    var right = join.getRight().accept(this);
    var condition = join.getCondition().flatMap(t -> visitExpression(t));
    var postFilter = join.getPostJoinFilter().flatMap(t -> visitExpression(t));
    if (allEmpty(left, right, condition, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableJoin.builder()
            .from(join)
            .left(left.orElse(join.getLeft()))
            .right(right.orElse(join.getRight()))
            .condition(condition.or(() -> join.getCondition()))
            .postJoinFilter(postFilter.or(() -> join.getPostJoinFilter()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Set set) throws RuntimeException {
    return transformList(set.getInputs(), t -> t.accept(this))
        .map(u -> ImmutableSet.builder().from(set).inputs(u).setOp(set.getSetOp()).build());
  }

  @Override
  public Optional<Rel> visit(Project project) throws RuntimeException {
    var input = project.getInput().accept(this);
    Optional<List<Expression>> expressions = transformExpressions(project.getExpressions());
    if (allEmpty(input, expressions)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableProject.builder()
            .from(project)
            .input(input.orElse(project.getInput()))
            .expressions(expressions.orElse(project.getExpressions()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Sort sort) throws RuntimeException {
    return sort.getInput()
        .accept(this)
        .map(input -> ImmutableSort.builder().from(sort).input(input).build());
  }

  @Override
  public Optional<Rel> visit(Cross cross) throws RuntimeException {
    var left = cross.getLeft().accept(this);
    var right = cross.getRight().accept(this);
    if (allEmpty(left, right)) {
      return Optional.empty();
    }
    Type.Struct unionedStruct =
        Type.Struct.builder()
            .from(left.orElse(cross.getLeft()).getRecordType())
            .from(right.orElse(cross.getRight()).getRecordType())
            .build();
    return Optional.of(
        ImmutableCross.builder()
            .from(cross)
            .left(left.orElse(cross.getLeft()))
            .right(right.orElse(cross.getRight()))
            .deriveRecordType(unionedStruct)
            .build());
  }

  private Optional<Expression> visitExpression(Expression expression) {
    ExpressionVisitor<Optional<Expression>, RuntimeException> visitor =
        new AbstractExpressionVisitor<>() {
          @Override
          public Optional<Expression> visitFallback(Expression expr) {
            return Optional.empty();
          }

          @Override
          public Optional<Expression> visit(Expression.Switch expr) throws RuntimeException {
            var defaultClause = expr.defaultClause().accept(this);
            var switchClauses =
                transformList(
                    expr.switchClauses(),
                    t ->
                        t.then()
                            .accept(this)
                            .map(u -> Expression.SwitchClause.builder().from(t).then(u).build()));
            if (allEmpty(defaultClause, switchClauses)) {
              return Optional.empty();
            }
            return Optional.of(
                Expression.Switch.builder()
                    .from(expr)
                    .defaultClause(defaultClause.orElse(expr.defaultClause()))
                    .switchClauses(switchClauses.orElse(expr.switchClauses()))
                    .build());
          }

          @Override
          public Optional<Expression> visit(Expression.IfThen expr) throws RuntimeException {
            var ifClauses =
                transformList(
                    expr.ifClauses(),
                    t ->
                        t.condition()
                            .accept(this)
                            .map(u -> Expression.IfClause.builder().from(t).condition(u).build()));
            var ifThenClauses =
                transformList(
                    ifClauses.orElse(expr.ifClauses()),
                    t ->
                        t.then()
                            .accept(this)
                            .map(u -> Expression.IfClause.builder().from(t).then(u).build()));
            var elseClause = expr.elseClause().accept(this);
            if (allEmpty(ifClauses, ifThenClauses, elseClause)) {
              return Optional.empty();
            }
            return Optional.of(
                Expression.IfThen.builder()
                    .from(expr)
                    .ifClauses(ifThenClauses.orElse(expr.ifClauses()))
                    .elseClause(elseClause.orElse(expr.elseClause()))
                    .build());
          }

          @Override
          public Optional<Expression> visit(Expression.ScalarFunctionInvocation expr)
              throws RuntimeException {
            return transformFuncArgs(expr.arguments())
                .map(
                    t ->
                        Expression.ScalarFunctionInvocation.builder()
                            .from(expr)
                            .arguments(t)
                            .build());
          }

          @Override
          public Optional<Expression> visit(Expression.Cast expr) throws RuntimeException {
            return expr.input()
                .accept(this)
                .map(t -> Expression.Cast.builder().from(expr).input(t).build());
          }

          @Override
          public Optional<Expression> visit(Expression.SingleOrList expr) throws RuntimeException {
            var condition = expr.condition().accept(this);
            var options = transformExpressions(expr.options());
            if (allEmpty(condition, options)) {
              return Optional.empty();
            }
            return Optional.of(
                Expression.SingleOrList.builder()
                    .from(expr)
                    .condition(condition.orElse(expr.condition()))
                    .options(options.orElse(expr.options()))
                    .build());
          }

          @Override
          public Optional<Expression> visit(Expression.MultiOrList expr) throws RuntimeException {
            var options = transformExpressions(expr.conditions());
            var multiOrListRecords =
                transformList(
                    expr.optionCombinations(),
                    t ->
                        transformExpressions(t.values())
                            .map(u -> Expression.MultiOrListRecord.builder().values(u).build()));
            if (allEmpty(options, multiOrListRecords)) {
              return Optional.empty();
            }
            return Optional.of(
                Expression.MultiOrList.builder()
                    .from(expr)
                    .optionCombinations(multiOrListRecords.orElse(expr.optionCombinations()))
                    .build());
          }

          @Override
          public Optional<Expression> visit(FieldReference expr) throws RuntimeException {
            return expr.inputExpression()
                .flatMap(t -> t.accept(this))
                .map(
                    t ->
                        ImmutableFieldReference.builder()
                            .inputExpression(Optional.ofNullable(t))
                            .build());
          }

          @Override
          public Optional<Expression> visit(Expression.SetPredicate expr) throws RuntimeException {
            return expr.tuples()
                .accept(RelCopyOnWriteVisitor.this)
                .map(t -> Expression.SetPredicate.builder().from(expr).tuples(t).build());
          }

          @Override
          public Optional<Expression> visit(Expression.ScalarSubquery expr)
              throws RuntimeException {
            return expr.input()
                .accept(RelCopyOnWriteVisitor.this)
                .map(t -> Expression.ScalarSubquery.builder().from(expr).input(t).build());
          }

          @Override
          public Optional<Expression> visit(Expression.InPredicate expr) throws RuntimeException {
            return expr.haystack()
                .accept(RelCopyOnWriteVisitor.this)
                .map(t -> Expression.InPredicate.builder().from(expr).haystack(t).build());
          }

          // TODO:
        };
    return expression.accept(visitor);
  }
}
