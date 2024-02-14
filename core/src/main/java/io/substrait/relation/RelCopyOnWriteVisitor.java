package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.or;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Class used to visit all child relations from a root relation and optionally replace subtrees by
 * overriding a visitor method. The traversal will include relations inside of subquery expressions.
 * By default, no subtree substitution will be performed. However, if a visit method is overridden
 * to return a non-empty optional value, then that value will replace the relation in the tree.
 */
public class RelCopyOnWriteVisitor<EXCEPTION extends Exception>
    implements RelVisitor<Optional<Rel>, EXCEPTION> {

  private final ExpressionCopyOnWriteVisitor<EXCEPTION> expressionCopyOnWriteVisitor;

  public RelCopyOnWriteVisitor() {
    this.expressionCopyOnWriteVisitor = new ExpressionCopyOnWriteVisitor<>(this);
  }

  public RelCopyOnWriteVisitor(
      ExpressionCopyOnWriteVisitor<EXCEPTION> expressionCopyOnWriteVisitor) {
    this.expressionCopyOnWriteVisitor = expressionCopyOnWriteVisitor;
  }

  public RelCopyOnWriteVisitor(
      Function<RelCopyOnWriteVisitor<EXCEPTION>, ExpressionCopyOnWriteVisitor<EXCEPTION>> fn) {
    this.expressionCopyOnWriteVisitor = fn.apply(this);
  }

  protected ExpressionCopyOnWriteVisitor<EXCEPTION> getExpressionCopyOnWriteVisitor() {
    return expressionCopyOnWriteVisitor;
  }

  @Override
  public Optional<Rel> visit(Aggregate aggregate) throws EXCEPTION {
    var input = aggregate.getInput().accept(this);
    var groupings = transformList(aggregate.getGroupings(), this::visitGrouping);
    var measures = transformList(aggregate.getMeasures(), this::visitMeasure);

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

  protected Optional<Aggregate.Grouping> visitGrouping(Aggregate.Grouping grouping)
      throws EXCEPTION {
    return visitExprList(grouping.getExpressions())
        .map(exprs -> Aggregate.Grouping.builder().from(grouping).expressions(exprs).build());
  }

  protected Optional<Aggregate.Measure> visitMeasure(Aggregate.Measure measure) throws EXCEPTION {
    var preMeasureFilter = visitOptionalExpression(measure.getPreMeasureFilter());
    var afi = visitAggregateFunction(measure.getFunction());

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
      AggregateFunctionInvocation afi) throws EXCEPTION {
    var arguments = visitFunctionArguments(afi.arguments());
    var sort = transformList(afi.sort(), this::visitSortField);

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
  public Optional<Rel> visit(EmptyScan emptyScan) throws EXCEPTION {
    Optional<Expression> filter = visitOptionalExpression(emptyScan.getFilter());

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        EmptyScan.builder()
            .from(emptyScan)
            .filter(filter.isPresent() ? filter : emptyScan.getFilter())
            .build());
  }

  @Override
  public Optional<Rel> visit(Fetch fetch) throws EXCEPTION {
    return fetch
        .getInput()
        .accept(this)
        .map(input -> Fetch.builder().from(fetch).input(input).build());
  }

  @Override
  public Optional<Rel> visit(Filter filter) throws EXCEPTION {
    var input = filter.getInput().accept(this);
    var condition = filter.getCondition().accept(getExpressionCopyOnWriteVisitor());

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
  public Optional<Rel> visit(Join join) throws EXCEPTION {
    var left = join.getLeft().accept(this);
    var right = join.getRight().accept(this);
    var condition = visitOptionalExpression(join.getCondition());
    var postFilter = visitOptionalExpression(join.getPostJoinFilter());

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
  public Optional<Rel> visit(Set set) throws EXCEPTION {
    return transformList(set.getInputs(), t -> t.accept(this))
        .map(s -> Set.builder().from(set).inputs(s).build());
  }

  @Override
  public Optional<Rel> visit(NamedScan namedScan) throws EXCEPTION {
    var filter = visitOptionalExpression(namedScan.getFilter());

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        NamedScan.builder().from(namedScan).filter(or(filter, namedScan::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(LocalFiles localFiles) throws EXCEPTION {
    var filter = visitOptionalExpression(localFiles.getFilter());

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        LocalFiles.builder().from(localFiles).filter(or(filter, localFiles::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(Project project) throws EXCEPTION {
    var input = project.getInput().accept(this);
    var expressions = visitExprList(project.getExpressions());

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
  public Optional<Rel> visit(Sort sort) throws EXCEPTION {
    var input = sort.getInput().accept(this);
    var sortFields = transformList(sort.getSortFields(), this::visitSortField);

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
  public Optional<Rel> visit(Cross cross) throws EXCEPTION {
    var left = cross.getLeft().accept(this);
    var right = cross.getRight().accept(this);

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
  public Optional<Rel> visit(VirtualTableScan virtualTableScan) throws EXCEPTION {
    var filter = visitOptionalExpression(virtualTableScan.getFilter());

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
  public Optional<Rel> visit(ExtensionLeaf extensionLeaf) throws EXCEPTION {
    return Optional.empty();
  }

  @Override
  public Optional<Rel> visit(ExtensionSingle extensionSingle) throws EXCEPTION {
    return extensionSingle
        .getInput()
        .accept(this)
        .map(input -> ExtensionSingle.builder().from(extensionSingle).input(input).build());
  }

  @Override
  public Optional<Rel> visit(ExtensionMulti extensionMulti) throws EXCEPTION {
    return transformList(extensionMulti.getInputs(), rel -> rel.accept(this))
        .map(inputs -> ExtensionMulti.builder().from(extensionMulti).inputs(inputs).build());
  }

  @Override
  public Optional<Rel> visit(ExtensionTable extensionTable) throws EXCEPTION {
    var filter = visitOptionalExpression(extensionTable.getFilter());

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
  public Optional<Rel> visit(HashJoin hashJoin) throws EXCEPTION {
    var left = hashJoin.getLeft().accept(this);
    var right = hashJoin.getRight().accept(this);
    var leftKeys = transformList(hashJoin.getLeftKeys(), this::visitFieldReference);
    var rightKeys = transformList(hashJoin.getRightKeys(), this::visitFieldReference);
    var postFilter = visitOptionalExpression(hashJoin.getPostJoinFilter());

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
  public Optional<Rel> visit(MergeJoin mergeJoin) throws EXCEPTION {
    var left = mergeJoin.getLeft().accept(this);
    var right = mergeJoin.getRight().accept(this);
    var leftKeys = transformList(mergeJoin.getLeftKeys(), this::visitFieldReference);
    var rightKeys = transformList(mergeJoin.getRightKeys(), this::visitFieldReference);
    var postFilter = visitOptionalExpression(mergeJoin.getPostJoinFilter());

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
  public Optional<Rel> visit(NestedLoopJoin nestedLoopJoin) throws EXCEPTION {
    var left = nestedLoopJoin.getLeft().accept(this);
    var right = nestedLoopJoin.getRight().accept(this);
    var condition = nestedLoopJoin.getCondition().accept(getExpressionCopyOnWriteVisitor());

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
  public Optional<Rel> visit(ConsistentPartitionWindow consistentPartitionWindow) throws EXCEPTION {
    var windowFunctions =
        transformList(consistentPartitionWindow.getWindowFunctions(), this::visitWindowRelFunction);
    var partitionExpressions =
        transformList(
            consistentPartitionWindow.getPartitionExpressions(),
            t -> t.accept(getExpressionCopyOnWriteVisitor()));
    var sorts = transformList(consistentPartitionWindow.getSorts(), this::visitSortField);

    return Optional.of(
        ConsistentPartitionWindow.builder()
            .from(consistentPartitionWindow)
            .partitionExpressions(
                partitionExpressions.orElse(consistentPartitionWindow.getPartitionExpressions()))
            .sorts(sorts.orElse(consistentPartitionWindow.getSorts()))
            .windowFunctions(windowFunctions.orElse(consistentPartitionWindow.getWindowFunctions()))
            .build());
  }

  // utilities

  protected Optional<List<Expression>> visitExprList(List<Expression> exprs) throws EXCEPTION {
    return transformList(exprs, t -> t.accept(getExpressionCopyOnWriteVisitor()));
  }

  public Optional<FieldReference> visitFieldReference(FieldReference fieldReference)
      throws EXCEPTION {
    var inputExpression = visitOptionalExpression(fieldReference.inputExpression());
    if (allEmpty(inputExpression)) {
      return Optional.empty();
    }

    return Optional.of(FieldReference.builder().inputExpression(inputExpression).build());
  }

  protected Optional<List<FunctionArg>> visitFunctionArguments(List<FunctionArg> funcArgs)
      throws EXCEPTION {
    return CopyOnWriteUtils.<FunctionArg, EXCEPTION>transformList(
        funcArgs,
        arg -> {
          if (arg instanceof Expression expr) {
            return expr.accept(getExpressionCopyOnWriteVisitor())
                .flatMap(Optional::<FunctionArg>of);
          } else {
            return Optional.empty();
          }
        });
  }

  protected Optional<Expression.SortField> visitSortField(Expression.SortField sortField)
      throws EXCEPTION {
    return sortField
        .expr()
        .accept(getExpressionCopyOnWriteVisitor())
        .map(expr -> Expression.SortField.builder().from(sortField).expr(expr).build());
  }

  protected Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> visitWindowRelFunction(
      ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunctionInvocation)
      throws EXCEPTION {
    return Optional.of(
        ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
            .arguments(
                visitFunctionArguments(windowRelFunctionInvocation.arguments())
                    .orElse(windowRelFunctionInvocation.arguments()))
            .declaration(windowRelFunctionInvocation.declaration())
            .outputType(windowRelFunctionInvocation.outputType())
            .aggregationPhase(windowRelFunctionInvocation.aggregationPhase())
            .invocation(windowRelFunctionInvocation.invocation())
            .lowerBound(windowRelFunctionInvocation.lowerBound())
            .upperBound(windowRelFunctionInvocation.upperBound())
            .boundsType(windowRelFunctionInvocation.boundsType())
            .build());
  }

  private Optional<Expression> visitOptionalExpression(Optional<Expression> optExpr)
      throws EXCEPTION {
    // not using optExpr.map to allow us to propagate the THROWABLE nicely
    if (optExpr.isPresent()) {
      return optExpr.get().accept(getExpressionCopyOnWriteVisitor());
    }
    return Optional.empty();
  }
}
