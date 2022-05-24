package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;

/**
 * Class used to visit all child relations from a root relation. The traversal will include
 * relations inside of subquery expressions.
 */
public class RelCopyOnWriteVisitor implements RelVisitor<Rel, RuntimeException> {
  @Override
  public Aggregate visit(Aggregate aggregate) {
    aggregate.getInput().accept(this);
    return aggregate;
  }

  @Override
  public EmptyScan visit(EmptyScan emptyScan) {
    return emptyScan;
  }

  @Override
  public Fetch visit(Fetch fetch) {
    fetch.getInput().accept(this);
    return fetch;
  }

  @Override
  public Filter visit(Filter filter) {
    filter.getInput().accept(this);
    visitExpression(filter.getCondition());
    return filter;
  }

  @Override
  public Join visit(Join join) {
    join.getLeft().accept(this);
    join.getRight().accept(this);
    if (join.getCondition().isPresent()) {
      visitExpression(join.getCondition().get());
    }
    if (join.getPostJoinFilter().isPresent()) {
      visitExpression(join.getPostJoinFilter().get());
    }
    return join;
  }

  @Override
  public NamedScan visit(NamedScan namedScan) {
    return namedScan;
  }

  @Override
  public Project visit(Project project) {
    project.getInput().accept(this);
    for (var expr : project.getExpressions()) {
      visitExpression(expr);
    }
    return project;
  }

  @Override
  public Sort visit(Sort sort) {
    sort.getInput().accept(this);
    return sort;
  }

  @Override
  public VirtualTableScan visit(VirtualTableScan virtualTableScan) {
    return virtualTableScan;
  }

  private Expression visitExpression(Expression expression) {
    ExpressionVisitor<Expression, RuntimeException> visitor =
        new ExpressionVisitor<Expression, RuntimeException>() {
          @Override
          public Expression.NullLiteral visit(Expression.NullLiteral expr) {
            return expr;
          }

          @Override
          public Expression.BoolLiteral visit(Expression.BoolLiteral expr) {
            return expr;
          }

          @Override
          public Expression.I8Literal visit(Expression.I8Literal expr) {
            return expr;
          }

          @Override
          public Expression.I16Literal visit(Expression.I16Literal expr) {
            return expr;
          }

          @Override
          public Expression.I32Literal visit(Expression.I32Literal expr) {
            return expr;
          }

          @Override
          public Expression.I64Literal visit(Expression.I64Literal expr) {
            return expr;
          }

          @Override
          public Expression.FP32Literal visit(Expression.FP32Literal expr) {
            return expr;
          }

          @Override
          public Expression.FP64Literal visit(Expression.FP64Literal expr) {
            return expr;
          }

          @Override
          public Expression.StrLiteral visit(Expression.StrLiteral expr) {
            return expr;
          }

          @Override
          public Expression.BinaryLiteral visit(Expression.BinaryLiteral expr) {
            return expr;
          }

          @Override
          public Expression.TimeLiteral visit(Expression.TimeLiteral expr) {
            return expr;
          }

          @Override
          public Expression.DateLiteral visit(Expression.DateLiteral expr) {
            return expr;
          }

          @Override
          public Expression.TimestampLiteral visit(Expression.TimestampLiteral expr) {
            return expr;
          }

          @Override
          public Expression.TimestampTZLiteral visit(Expression.TimestampTZLiteral expr) {
            return expr;
          }

          @Override
          public Expression.IntervalYearLiteral visit(Expression.IntervalYearLiteral expr) {
            return expr;
          }

          @Override
          public Expression.IntervalDayLiteral visit(Expression.IntervalDayLiteral expr) {
            return expr;
          }

          @Override
          public Expression.UUIDLiteral visit(Expression.UUIDLiteral expr) {
            return expr;
          }

          @Override
          public Expression.FixedCharLiteral visit(Expression.FixedCharLiteral expr) {
            return expr;
          }

          @Override
          public Expression.VarCharLiteral visit(Expression.VarCharLiteral expr) {
            return expr;
          }

          @Override
          public Expression.FixedBinaryLiteral visit(Expression.FixedBinaryLiteral expr) {
            return expr;
          }

          @Override
          public Expression.DecimalLiteral visit(Expression.DecimalLiteral expr) {
            return expr;
          }

          @Override
          public Expression.MapLiteral visit(Expression.MapLiteral expr) {
            return expr;
          }

          @Override
          public Expression.ListLiteral visit(Expression.ListLiteral expr) {
            return expr;
          }

          @Override
          public Expression.StructLiteral visit(Expression.StructLiteral expr) {
            return expr;
          }

          @Override
          public Expression.Switch visit(Expression.Switch expr) {
            expr.defaultClause().accept(this);
            for (var clause : expr.switchClauses()) {
              clause.then().accept(this);
            }
            return expr;
          }

          @Override
          public Expression.IfThen visit(Expression.IfThen expr) {
            for (var clause : expr.ifClauses()) {
              clause.condition().accept(this);
              clause.then().accept(this);
            }
            expr.elseClause().accept(this);
            return expr;
          }

          @Override
          public Expression.ScalarFunctionInvocation visit(
              Expression.ScalarFunctionInvocation expr) {
            for (var arg : expr.arguments()) {
              arg.accept(this);
            }
            return expr;
          }

          @Override
          public Expression.Cast visit(Expression.Cast expr) {
            expr.input().accept(this);
            return expr;
          }

          @Override
          public Expression.SingleOrList visit(Expression.SingleOrList expr) {
            expr.condition().accept(this);
            for (var option : expr.options()) {
              option.accept(this);
            }
            return expr;
          }

          @Override
          public Expression.MultiOrList visit(Expression.MultiOrList expr) {
            for (var condition : expr.conditions()) {
              condition.accept(this);
            }
            for (var optionCombos : expr.optionCombinations()) {
              for (var option : optionCombos.values()) {
                option.accept(this);
              }
            }
            return expr;
          }

          @Override
          public FieldReference visit(FieldReference expr) {
            if (expr.inputExpression().isPresent()) {
              expr.inputExpression().get().accept(this);
            }
            // TODO: other children to traverse?
            return expr;
          }

          @Override
          public Expression.SetPredicate visit(Expression.SetPredicate expr) {
            expr.tuples().accept(RelCopyOnWriteVisitor.this);
            return expr;
          }

          @Override
          public Expression.ScalarSubquery visit(Expression.ScalarSubquery expr) {
            expr.input().accept(RelCopyOnWriteVisitor.this);
            return expr;
          }

          @Override
          public Expression.InPredicate visit(Expression.InPredicate expr) {
            expr.haystack().accept(RelCopyOnWriteVisitor.this);
            return expr;
          }
        };
    return expression.accept(visitor);
  }
}
