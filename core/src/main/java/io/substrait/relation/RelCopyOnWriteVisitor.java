package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.or;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Class used to visit all child relations from a root relation and optionally replace subtrees by
 * overriding a visitor method. The traversal will include relations inside of subquery expressions.
 * By default, no subtree substitution will be performed. However, if a visit method is overridden
 * to return a non-empty optional value, then that value will replace the relation in the tree.
 */
public class RelCopyOnWriteVisitor<E extends Exception>
    implements RelVisitor<Optional<Rel>, EmptyVisitationContext, E> {

  private final ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor;

  public RelCopyOnWriteVisitor() {
    this.expressionCopyOnWriteVisitor = new ExpressionCopyOnWriteVisitor<>(this);
  }

  public RelCopyOnWriteVisitor(ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor) {
    this.expressionCopyOnWriteVisitor = expressionCopyOnWriteVisitor;
  }

  public RelCopyOnWriteVisitor(
      Function<RelCopyOnWriteVisitor<E>, ExpressionCopyOnWriteVisitor<E>> fn) {
    this.expressionCopyOnWriteVisitor = fn.apply(this);
  }

  protected ExpressionCopyOnWriteVisitor<E> getExpressionCopyOnWriteVisitor() {
    return expressionCopyOnWriteVisitor;
  }

  @Override
  public Optional<Rel> visit(Aggregate aggregate, EmptyVisitationContext context) throws E {
    Optional<Rel> input = aggregate.getInput().accept(this, context);
    Optional<List<Aggregate.Grouping>> groupings =
        transformList(aggregate.getGroupings(), context, this::visitGrouping);
    Optional<List<Aggregate.Measure>> measures =
        transformList(aggregate.getMeasures(), context, this::visitMeasure);

    if (allEmpty(input, groupings, measures)) {
      return Optional.empty();
    }
    return Optional.of(
        Aggregate.builder()
            .from(aggregate)
            .input(input.orElse(aggregate.getInput()))
            .groupings(groupings.orElse(aggregate.getGroupings()))
            .measures(measures.orElse(aggregate.getMeasures()))
            .build());
  }

  protected Optional<Aggregate.Grouping> visitGrouping(
      Aggregate.Grouping grouping, EmptyVisitationContext context) throws E {
    return visitExprList(grouping.getExpressions(), context)
        .map(exprs -> Aggregate.Grouping.builder().from(grouping).expressions(exprs).build());
  }

  protected Optional<Aggregate.Measure> visitMeasure(
      Aggregate.Measure measure, EmptyVisitationContext context) throws E {
    Optional<Expression> preMeasureFilter =
        visitOptionalExpression(measure.getPreMeasureFilter(), context);
    Optional<AggregateFunctionInvocation> afi =
        visitAggregateFunction(measure.getFunction(), context);

    if (allEmpty(preMeasureFilter, afi)) {
      return Optional.empty();
    }
    return Optional.of(
        Aggregate.Measure.builder()
            .from(measure)
            .preMeasureFilter(or(preMeasureFilter, measure::getPreMeasureFilter))
            .function(afi.orElse(measure.getFunction()))
            .build());
  }

  protected Optional<AggregateFunctionInvocation> visitAggregateFunction(
      AggregateFunctionInvocation afi, EmptyVisitationContext context) throws E {
    Optional<List<FunctionArg>> arguments = visitFunctionArguments(afi.arguments(), context);
    Optional<List<Expression.SortField>> sort =
        transformList(afi.sort(), context, this::visitSortField);

    if (allEmpty(arguments, sort)) {
      return Optional.empty();
    }
    return Optional.of(
        AggregateFunctionInvocation.builder()
            .from(afi)
            .arguments(arguments.orElse(afi.arguments()))
            .sort(sort.orElse(afi.sort()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Fetch fetch, EmptyVisitationContext context) throws E {
    return fetch
        .getInput()
        .accept(this, context)
        .map(input -> Fetch.builder().from(fetch).input(input).build());
  }

  @Override
  public Optional<Rel> visit(Filter filter, EmptyVisitationContext context) throws E {
    Optional<Rel> input = filter.getInput().accept(this, context);
    Optional<Expression> condition =
        filter.getCondition().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(input, condition)) {
      return Optional.empty();
    }
    return Optional.of(
        Filter.builder()
            .from(filter)
            .input(input.orElse(filter.getInput()))
            .condition(condition.orElse(filter.getCondition()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Join join, EmptyVisitationContext context) throws E {
    Optional<Rel> left = join.getLeft().accept(this, context);
    Optional<Rel> right = join.getRight().accept(this, context);
    Optional<Expression> condition = visitOptionalExpression(join.getCondition(), context);
    Optional<Expression> postFilter = visitOptionalExpression(join.getPostJoinFilter(), context);

    if (allEmpty(left, right, condition, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableJoin.builder()
            .from(join)
            .left(left.orElse(join.getLeft()))
            .right(right.orElse(join.getRight()))
            .condition(or(condition, join::getCondition))
            .postJoinFilter(or(postFilter, join::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(Set set, EmptyVisitationContext context) throws E {
    return transformList(set.getInputs(), context, (t, c) -> t.accept(this, c))
        .map(s -> Set.builder().from(set).inputs(s).build());
  }

  @Override
  public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) throws E {
    Optional<Expression> filter = visitOptionalExpression(namedScan.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        NamedScan.builder().from(namedScan).filter(or(filter, namedScan::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(LocalFiles localFiles, EmptyVisitationContext context) throws E {
    Optional<Expression> filter = visitOptionalExpression(localFiles.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        LocalFiles.builder().from(localFiles).filter(or(filter, localFiles::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(Project project, EmptyVisitationContext context) throws E {
    Optional<Rel> input = project.getInput().accept(this, context);
    Optional<List<Expression>> expressions = visitExprList(project.getExpressions(), context);

    if (allEmpty(input, expressions)) {
      return Optional.empty();
    }
    return Optional.of(
        Project.builder()
            .from(project)
            .input(input.orElse(project.getInput()))
            .expressions(expressions.orElse(project.getExpressions()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Expand expand, EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(NamedWrite write, EmptyVisitationContext context) throws E {

    Optional<Rel> input = write.getInput().accept(this, context);

    if (allEmpty(input)) {
      return Optional.empty();
    }

    return Optional.of(
        NamedWrite.builder().from(write).input(input.orElse(write.getInput())).build());
  }

  @Override
  public Optional<Rel> visit(ExtensionWrite write, EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(NamedDdl ddl, EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Rel> visit(ExtensionDdl ddl, EmptyVisitationContext context) throws E {
    throw new UnsupportedOperationException();
  }

  protected Optional<NamedUpdate.TransformExpression> visitTransformExpression(
      NamedUpdate.TransformExpression transform, EmptyVisitationContext context) throws E {
    return transform
        .getTransformation()
        .accept(getExpressionCopyOnWriteVisitor(), context)
        .map(
            expr ->
                NamedUpdate.TransformExpression.builder()
                    .from(transform)
                    .transformation(expr)
                    .build());
  }

  @Override
  public Optional<Rel> visit(NamedUpdate update, EmptyVisitationContext context) throws E {
    Optional<Expression> condition =
        update.getCondition().accept(getExpressionCopyOnWriteVisitor(), context);

    Optional<List<AbstractUpdate.TransformExpression>> transformations =
        transformList(update.getTransformations(), context, this::visitTransformExpression);

    if (allEmpty(condition, transformations)) {
      return Optional.empty();
    }

    return Optional.of(
        NamedUpdate.builder()
            .from(update)
            .condition(condition.orElse(update.getCondition()))
            .transformations(transformations.orElse(update.getTransformations()))
            .build());
  }

  @Override
  public Optional<Rel> visit(ScatterExchange exchange, EmptyVisitationContext context) throws E {
    Optional<Rel> input = exchange.getInput().accept(this, context);
    Optional<List<FieldReference>> fields =
        transformList(exchange.getFields(), context, this::visitFieldReference);

    if (allEmpty(input, fields)) {
      return Optional.empty();
    }

    return Optional.of(
        ScatterExchange.builder()
            .from(exchange)
            .input(input.orElse(exchange.getInput()))
            .fields(fields.orElse(exchange.getFields()))
            .build());
  }

  @Override
  public Optional<Rel> visit(SingleBucketExchange exchange, EmptyVisitationContext context)
      throws E {
    Optional<Rel> input = exchange.getInput().accept(this, context);

    Optional<Expression> expression =
        exchange.getExpression().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(input, expression)) {
      return Optional.empty();
    }

    return Optional.of(
        SingleBucketExchange.builder()
            .from(exchange)
            .input(input.orElse(exchange.getInput()))
            .expression(expression.orElse(exchange.getExpression()))
            .build());
  }

  @Override
  public Optional<Rel> visit(MultiBucketExchange exchange, EmptyVisitationContext context)
      throws E {
    Optional<Rel> input = exchange.getInput().accept(this, context);
    Optional<Expression> expression =
        exchange.getExpression().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(input)) {
      return Optional.empty();
    }

    return Optional.of(
        MultiBucketExchange.builder()
            .from(exchange)
            .input(input.orElse(exchange.getInput()))
            .expression(expression.orElse(exchange.getExpression()))
            .build());
  }

  @Override
  public Optional<Rel> visit(RoundRobinExchange exchange, EmptyVisitationContext context) throws E {
    Optional<Rel> input = exchange.getInput().accept(this, context);
    if (allEmpty(input)) {
      return Optional.empty();
    }

    return Optional.of(
        RoundRobinExchange.builder()
            .from(exchange)
            .input(input.orElse(exchange.getInput()))
            .build());
  }

  @Override
  public Optional<Rel> visit(BroadcastExchange exchange, EmptyVisitationContext context) throws E {
    Optional<Rel> input = exchange.getInput().accept(this, context);
    if (allEmpty(input)) {
      return Optional.empty();
    }

    return Optional.of(
        BroadcastExchange.builder()
            .from(exchange)
            .input(input.orElse(exchange.getInput()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Sort sort, EmptyVisitationContext context) throws E {
    Optional<Rel> input = sort.getInput().accept(this, context);
    Optional<List<Expression.SortField>> sortFields =
        transformList(sort.getSortFields(), context, this::visitSortField);

    if (allEmpty(input, sortFields)) {
      return Optional.empty();
    }
    return Optional.of(
        Sort.builder()
            .from(sort)
            .input(input.orElse(sort.getInput()))
            .sortFields(sortFields.orElse(sort.getSortFields()))
            .build());
  }

  @Override
  public Optional<Rel> visit(Cross cross, EmptyVisitationContext context) throws E {
    Optional<Rel> left = cross.getLeft().accept(this, context);
    Optional<Rel> right = cross.getRight().accept(this, context);

    if (allEmpty(left, right)) {
      return Optional.empty();
    }
    return Optional.of(
        Cross.builder()
            .from(cross)
            .left(left.orElse(cross.getLeft()))
            .right(right.orElse(cross.getRight()))
            .build());
  }

  @Override
  public Optional<Rel> visit(VirtualTableScan virtualTableScan, EmptyVisitationContext context)
      throws E {
    Optional<Expression> filter = visitOptionalExpression(virtualTableScan.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        VirtualTableScan.builder()
            .from(virtualTableScan)
            .filter(or(filter, virtualTableScan::getFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(ExtensionLeaf extensionLeaf, EmptyVisitationContext context) throws E {
    return Optional.empty();
  }

  @Override
  public Optional<Rel> visit(ExtensionSingle extensionSingle, EmptyVisitationContext context)
      throws E {
    return extensionSingle
        .getInput()
        .accept(this, context)
        .map(input -> ExtensionSingle.builder().from(extensionSingle).input(input).build());
  }

  @Override
  public Optional<Rel> visit(ExtensionMulti extensionMulti, EmptyVisitationContext context)
      throws E {
    return transformList(extensionMulti.getInputs(), context, (rel, c) -> rel.accept(this, c))
        .map(inputs -> ExtensionMulti.builder().from(extensionMulti).inputs(inputs).build());
  }

  @Override
  public Optional<Rel> visit(ExtensionTable extensionTable, EmptyVisitationContext context)
      throws E {
    Optional<Expression> filter = visitOptionalExpression(extensionTable.getFilter(), context);

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        ExtensionTable.builder()
            .from(extensionTable)
            .filter(or(filter, extensionTable::getFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(HashJoin hashJoin, EmptyVisitationContext context) throws E {
    Optional<Rel> left = hashJoin.getLeft().accept(this, context);
    Optional<Rel> right = hashJoin.getRight().accept(this, context);
    Optional<List<FieldReference>> leftKeys =
        transformList(hashJoin.getLeftKeys(), context, this::visitFieldReference);
    Optional<List<FieldReference>> rightKeys =
        transformList(hashJoin.getRightKeys(), context, this::visitFieldReference);
    Optional<Expression> postFilter =
        visitOptionalExpression(hashJoin.getPostJoinFilter(), context);

    if (allEmpty(left, right, leftKeys, rightKeys, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        HashJoin.builder()
            .from(hashJoin)
            .left(left.orElse(hashJoin.getLeft()))
            .right(right.orElse(hashJoin.getRight()))
            .leftKeys(leftKeys.orElse(hashJoin.getLeftKeys()))
            .rightKeys(rightKeys.orElse(hashJoin.getRightKeys()))
            .postJoinFilter(or(postFilter, hashJoin::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(MergeJoin mergeJoin, EmptyVisitationContext context) throws E {
    Optional<Rel> left = mergeJoin.getLeft().accept(this, context);
    Optional<Rel> right = mergeJoin.getRight().accept(this, context);
    Optional<List<FieldReference>> leftKeys =
        transformList(mergeJoin.getLeftKeys(), context, this::visitFieldReference);
    Optional<List<FieldReference>> rightKeys =
        transformList(mergeJoin.getRightKeys(), context, this::visitFieldReference);
    Optional<Expression> postFilter =
        visitOptionalExpression(mergeJoin.getPostJoinFilter(), context);

    if (allEmpty(left, right, leftKeys, rightKeys, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        MergeJoin.builder()
            .from(mergeJoin)
            .left(left.orElse(mergeJoin.getLeft()))
            .right(right.orElse(mergeJoin.getRight()))
            .leftKeys(leftKeys.orElse(mergeJoin.getLeftKeys()))
            .rightKeys(rightKeys.orElse(mergeJoin.getRightKeys()))
            .postJoinFilter(or(postFilter, mergeJoin::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(NestedLoopJoin nestedLoopJoin, EmptyVisitationContext context)
      throws E {
    Optional<Rel> left = nestedLoopJoin.getLeft().accept(this, context);
    Optional<Rel> right = nestedLoopJoin.getRight().accept(this, context);
    Optional<Expression> condition =
        nestedLoopJoin.getCondition().accept(getExpressionCopyOnWriteVisitor(), context);

    if (allEmpty(left, right, condition)) {
      return Optional.empty();
    }
    return Optional.of(
        NestedLoopJoin.builder()
            .from(nestedLoopJoin)
            .left(left.orElse(nestedLoopJoin.getLeft()))
            .right(right.orElse(nestedLoopJoin.getRight()))
            .condition(condition.orElse(nestedLoopJoin.getCondition()))
            .build());
  }

  @Override
  public Optional<Rel> visit(
      ConsistentPartitionWindow consistentPartitionWindow, EmptyVisitationContext context)
      throws E {
    Optional<List<ConsistentPartitionWindow.WindowRelFunctionInvocation>> windowFunctions =
        transformList(
            consistentPartitionWindow.getWindowFunctions(), context, this::visitWindowRelFunction);
    Optional<List<Expression>> partitionExpressions =
        transformList(
            consistentPartitionWindow.getPartitionExpressions(),
            context,
            (t, c) -> t.accept(getExpressionCopyOnWriteVisitor(), c));
    Optional<List<Expression.SortField>> sorts =
        transformList(consistentPartitionWindow.getSorts(), context, this::visitSortField);

    if (allEmpty(windowFunctions, partitionExpressions, sorts)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.builder()
            .from(consistentPartitionWindow)
            .partitionExpressions(
                partitionExpressions.orElse(consistentPartitionWindow.getPartitionExpressions()))
            .sorts(sorts.orElse(consistentPartitionWindow.getSorts()))
            .windowFunctions(windowFunctions.orElse(consistentPartitionWindow.getWindowFunctions()))
            .build());
  }

  protected Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> visitWindowRelFunction(
      ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunctionInvocation,
      EmptyVisitationContext context)
      throws E {
    Optional<List<FunctionArg>> functionArgs =
        visitFunctionArguments(windowRelFunctionInvocation.arguments(), context);

    if (allEmpty(functionArgs)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
            .from(windowRelFunctionInvocation)
            .arguments(functionArgs.orElse(windowRelFunctionInvocation.arguments()))
            .build());
  }

  // utilities

  protected Optional<List<Expression>> visitExprList(
      List<Expression> exprs, EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (t, c) -> t.accept(getExpressionCopyOnWriteVisitor(), c));
  }

  public Optional<FieldReference> visitFieldReference(
      FieldReference fieldReference, EmptyVisitationContext context) throws E {
    Optional<Expression> inputExpression =
        visitOptionalExpression(fieldReference.inputExpression(), context);
    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }

    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  protected Optional<List<FunctionArg>> visitFunctionArguments(
      List<FunctionArg> funcArgs, EmptyVisitationContext context) throws E {
    return CopyOnWriteUtils.<FunctionArg, EmptyVisitationContext, E>transformList(
        funcArgs,
        context,
        (arg, c) -> {
          if (arg instanceof Expression) {
            return ((Expression) arg)
                .accept(getExpressionCopyOnWriteVisitor(), c)
                .flatMap(Optional::<FunctionArg>of);
          }

          return Optional.empty();
        });
  }

  protected Optional<Expression.SortField> visitSortField(
      Expression.SortField sortField, EmptyVisitationContext context) throws E {
    return sortField
        .expr()
        .accept(getExpressionCopyOnWriteVisitor(), context)
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }

  private Optional<Expression> visitOptionalExpression(
      Optional<Expression> optExpr, EmptyVisitationContext context) throws E {
    // not using optExpr.map to allow us to propagate the THROWABLE nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(getExpressionCopyOnWriteVisitor(), context);
    }
    return Optional.empty();
  }
}
