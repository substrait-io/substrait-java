package io.substrait.relation;

import static io.substrait.relation.CopyOnWriteUtils.allEmpty;
import static io.substrait.relation.CopyOnWriteUtils.or;
import static io.substrait.relation.CopyOnWriteUtils.transformList;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.relation.physical.TopN;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Class used to visit all child relations from a root relation and optionally replace subtrees by
 * overriding a visitor method. The traversal will include relations inside of subquery expressions.
 * By default, no subtree substitution will be performed. However, if a visit method is overridden
 * to return a non-empty optional value, then that value will replace the relation in the tree.
 *
 * <p>Replacing a subtree may change the record type it emits. Because a {@link FieldReference}
 * caches the type of the field it references, the references in the expressions of the relations
 * above the replaced subtree would otherwise be left with a stale type. To avoid that, this visitor
 * tracks the record type that each relation's own expressions resolve against and re-derives the
 * cached type of every field reference it rewrites from the input the reference resolves against.
 * The types cached on function invocations are not re-derived; deriving those requires the function
 * declarations, which this visitor does not have.
 *
 * <p>Tracking that scope makes a visitor instance stateful for the duration of a traversal, so an
 * instance must not be used to visit several relation trees concurrently.
 */
public class RelCopyOnWriteVisitor<E extends Exception>
    implements RelVisitor<Optional<Rel>, EmptyVisitationContext, E> {

  private final ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor;

  /**
   * The record types that the root {@link FieldReference}s of the expressions being rewritten
   * resolve against, innermost last. An entry is {@code null} when the expressions it covers do not
   * resolve against an input record type, as for the filter of a read relation, which resolves
   * against the schema being read.
   */
  private final List<Type.Struct> inputTypes = new ArrayList<>();

  /**
   * The record types of the enclosing scopes, one entry per subquery boundary crossed. An outer
   * reference stepping out {@code stepsOut} levels resolves against the entry {@code stepsOut} from
   * the top.
   */
  private final List<Type.Struct> outerInputTypes = new ArrayList<>();

  /**
   * The record type each rel anchor currently in scope exposes to the outer references identified
   * by it, keyed by anchor. A lateral join registers one for the duration of its right input's
   * rewrite: that input is where the references bound to the join's anchor live, and they resolve
   * against the record type its left input emits.
   */
  private final Map<Integer, Type.Struct> anchorScopes = new HashMap<>();

  /** Creates a visitor using a default expression visitor bound to this relation visitor. */
  public RelCopyOnWriteVisitor() {
    this.expressionCopyOnWriteVisitor = new ExpressionCopyOnWriteVisitor<>(this);
  }

  /**
   * Creates a visitor using the given expression visitor.
   *
   * @param expressionCopyOnWriteVisitor the expression visitor to delegate to
   */
  public RelCopyOnWriteVisitor(ExpressionCopyOnWriteVisitor<E> expressionCopyOnWriteVisitor) {
    this.expressionCopyOnWriteVisitor = expressionCopyOnWriteVisitor;
  }

  /**
   * Creates a visitor whose expression visitor is built from this instance by the given factory.
   *
   * @param fn factory producing the expression visitor from this relation visitor
   */
  public RelCopyOnWriteVisitor(
      Function<RelCopyOnWriteVisitor<E>, ExpressionCopyOnWriteVisitor<E>> fn) {
    this.expressionCopyOnWriteVisitor = fn.apply(this);
  }

  /**
   * Returns the expression visitor used to rewrite expressions within relations.
   *
   * @return the expression copy-on-write visitor
   */
  protected ExpressionCopyOnWriteVisitor<E> getExpressionCopyOnWriteVisitor() {
    return expressionCopyOnWriteVisitor;
  }

  @Override
  public Optional<Rel> visit(Aggregate aggregate, EmptyVisitationContext context) throws E {
    Optional<Rel> input = aggregate.getInput().accept(this, context);
    Type.Struct inputType = recordTypeOf(input.orElse(aggregate.getInput()));
    Optional<List<Aggregate.Grouping>> groupings =
        inInputScope(
            inputType, () -> transformList(aggregate.getGroupings(), context, this::visitGrouping));
    Optional<List<Aggregate.Measure>> measures =
        inInputScope(
            inputType, () -> transformList(aggregate.getMeasures(), context, this::visitMeasure));

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

  /**
   * Rewrites an aggregate grouping, returning a new grouping if any expression changed.
   *
   * @param grouping the grouping to rewrite
   * @param context the visitation context
   * @return the rewritten grouping, or empty if unchanged
   * @throws E if the visit fails
   */
  protected Optional<Aggregate.Grouping> visitGrouping(
      Aggregate.Grouping grouping, EmptyVisitationContext context) throws E {
    return visitExprList(grouping.getExpressions(), context)
        .map(exprs -> Aggregate.Grouping.builder().from(grouping).expressions(exprs).build());
  }

  /**
   * Rewrites an aggregate measure, returning a new measure if anything changed.
   *
   * @param measure the measure to rewrite
   * @param context the visitation context
   * @return the rewritten measure, or empty if unchanged
   * @throws E if the visit fails
   */
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

  /**
   * Rewrites an aggregate function invocation, returning a new one if anything changed.
   *
   * @param afi the aggregate function invocation to rewrite
   * @param context the visitation context
   * @return the rewritten invocation, or empty if unchanged
   * @throws E if the visit fails
   */
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
    Optional<Rel> input = fetch.getInput().accept(this, context);
    Type.Struct inputType = recordTypeOf(input.orElse(fetch.getInput()));
    Optional<Expression> offset =
        inInputScope(inputType, () -> visitOptionalExpression(fetch.getOffset(), context));
    Optional<Expression> count =
        inInputScope(inputType, () -> visitOptionalExpression(fetch.getCount(), context));

    if (allEmpty(input, offset, count)) {
      return Optional.empty();
    }
    return Optional.of(
        Fetch.builder()
            .from(fetch)
            .input(input.orElse(fetch.getInput()))
            .offset(or(offset, fetch::getOffset))
            .count(or(count, fetch::getCount))
            .build());
  }

  @Override
  public Optional<Rel> visit(Filter filter, EmptyVisitationContext context) throws E {
    Optional<Rel> input = filter.getInput().accept(this, context);
    Optional<Expression> condition =
        inInputScope(
            recordTypeOf(input.orElse(filter.getInput())),
            () -> filter.getCondition().accept(getExpressionCopyOnWriteVisitor(), context));

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
    Type.Struct inputType =
        recordTypeOf(left.orElse(join.getLeft()), right.orElse(join.getRight()));
    Optional<Expression> condition =
        inInputScope(inputType, () -> visitOptionalExpression(join.getCondition(), context));
    Optional<Expression> postFilter =
        inInputScope(inputType, () -> visitOptionalExpression(join.getPostJoinFilter(), context));

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
  public Optional<Rel> visit(LateralJoin lateralJoin, EmptyVisitationContext context) throws E {
    Optional<Rel> left = lateralJoin.getLeft().accept(this, context);
    Rel newLeft = left.orElse(lateralJoin.getLeft());
    // The right input is evaluated once per left row, and the references it makes to that row are
    // identified by this join's anchor, so the left record type is in scope under that anchor for
    // exactly the rewrite of the right input.
    Optional<Rel> right =
        inAnchorScope(
            lateralJoin.getRelAnchor(),
            recordTypeOf(newLeft),
            () -> lateralJoin.getRight().accept(this, context));
    Type.Struct inputType = recordTypeOf(newLeft, right.orElse(lateralJoin.getRight()));
    Optional<Expression> condition =
        inInputScope(inputType, () -> visitOptionalExpression(lateralJoin.getCondition(), context));
    Optional<Expression> postFilter =
        inInputScope(
            inputType, () -> visitOptionalExpression(lateralJoin.getPostJoinFilter(), context));

    if (allEmpty(left, right, condition, postFilter)) {
      return Optional.empty();
    }
    return Optional.of(
        ImmutableLateralJoin.builder()
            .from(lateralJoin)
            .left(newLeft)
            .right(right.orElse(lateralJoin.getRight()))
            .condition(or(condition, lateralJoin::getCondition))
            .postJoinFilter(or(postFilter, lateralJoin::getPostJoinFilter))
            .build());
  }

  @Override
  public Optional<Rel> visit(Set set, EmptyVisitationContext context) throws E {
    return transformList(set.getInputs(), context, (t, c) -> t.accept(this, c))
        .map(s -> Set.builder().from(set).inputs(s).build());
  }

  @Override
  public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) throws E {
    Optional<Expression> filter =
        outsideInputScope(() -> visitOptionalExpression(namedScan.getFilter(), context));

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        NamedScan.builder().from(namedScan).filter(or(filter, namedScan::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(LocalFiles localFiles, EmptyVisitationContext context) throws E {
    Optional<Expression> filter =
        outsideInputScope(() -> visitOptionalExpression(localFiles.getFilter(), context));

    if (allEmpty(filter)) {
      return Optional.empty();
    }
    return Optional.of(
        LocalFiles.builder().from(localFiles).filter(or(filter, localFiles::getFilter)).build());
  }

  @Override
  public Optional<Rel> visit(Project project, EmptyVisitationContext context) throws E {
    Optional<Rel> input = project.getInput().accept(this, context);
    Optional<List<Expression>> expressions =
        inInputScope(
            recordTypeOf(input.orElse(project.getInput())),
            () -> visitExprList(project.getExpressions(), context));

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

  /**
   * Rewrites a named-update transform expression, returning a new one if it changed.
   *
   * @param transform the transform expression to rewrite
   * @param context the visitation context
   * @return the rewritten transform expression, or empty if unchanged
   * @throws E if the visit fails
   */
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
        outsideInputScope(
            () -> update.getCondition().accept(getExpressionCopyOnWriteVisitor(), context));

    Optional<List<AbstractUpdate.TransformExpression>> transformations =
        outsideInputScope(
            () ->
                transformList(
                    update.getTransformations(), context, this::visitTransformExpression));

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
        inInputScope(
            recordTypeOf(input.orElse(exchange.getInput())),
            () -> transformList(exchange.getFields(), context, this::visitFieldReference));

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
        inInputScope(
            recordTypeOf(input.orElse(exchange.getInput())),
            () -> exchange.getExpression().accept(getExpressionCopyOnWriteVisitor(), context));

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
        inInputScope(
            recordTypeOf(input.orElse(exchange.getInput())),
            () -> exchange.getExpression().accept(getExpressionCopyOnWriteVisitor(), context));

    if (allEmpty(input, expression)) {
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
        inInputScope(
            recordTypeOf(input.orElse(sort.getInput())),
            () -> transformList(sort.getSortFields(), context, this::visitSortField));

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
  public Optional<Rel> visit(TopN topN, EmptyVisitationContext context) throws E {
    Optional<Rel> input = topN.getInput().accept(this, context);
    Type.Struct inputType = recordTypeOf(input.orElse(topN.getInput()));
    Optional<List<Expression.SortField>> sortFields =
        inInputScope(
            inputType, () -> transformList(topN.getSortFields(), context, this::visitSortField));
    Optional<Expression> offset =
        inInputScope(inputType, () -> visitOptionalExpression(topN.getOffset(), context));
    Optional<Expression> count =
        inInputScope(inputType, () -> visitOptionalExpression(topN.getCount(), context));

    if (allEmpty(input, sortFields, offset, count)) {
      return Optional.empty();
    }
    return Optional.of(
        TopN.builder()
            .from(topN)
            .input(input.orElse(topN.getInput()))
            .sortFields(sortFields.orElse(topN.getSortFields()))
            .offset(or(offset, topN::getOffset))
            .count(or(count, topN::getCount))
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
    Optional<Expression> filter =
        outsideInputScope(() -> visitOptionalExpression(virtualTableScan.getFilter(), context));

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
    Optional<Expression> filter =
        outsideInputScope(() -> visitOptionalExpression(extensionTable.getFilter(), context));

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
    Type.Struct leftType = recordTypeOf(left.orElse(hashJoin.getLeft()));
    Type.Struct rightType = recordTypeOf(right.orElse(hashJoin.getRight()));
    Type.Struct inputType =
        recordTypeOf(left.orElse(hashJoin.getLeft()), right.orElse(hashJoin.getRight()));
    Optional<List<ComparisonJoinKey>> keys =
        transformList(
            hashJoin.getKeys(),
            context,
            (key, c) -> visitComparisonJoinKey(key, leftType, rightType, c));
    Optional<Expression> postFilter =
        inInputScope(
            inputType, () -> visitOptionalExpression(hashJoin.getPostJoinFilter(), context));
    Optional<Expression> residual =
        inInputScope(
            inputType, () -> visitOptionalExpression(hashJoin.getResidualExpression(), context));

    if (allEmpty(left, right, keys, postFilter, residual)) {
      return Optional.empty();
    }
    return Optional.of(
        HashJoin.builder()
            .from(hashJoin)
            .left(left.orElse(hashJoin.getLeft()))
            .right(right.orElse(hashJoin.getRight()))
            .keys(keys.orElse(hashJoin.getKeys()))
            .postJoinFilter(or(postFilter, hashJoin::getPostJoinFilter))
            .residualExpression(or(residual, hashJoin::getResidualExpression))
            .build());
  }

  @Override
  public Optional<Rel> visit(MergeJoin mergeJoin, EmptyVisitationContext context) throws E {
    Optional<Rel> left = mergeJoin.getLeft().accept(this, context);
    Optional<Rel> right = mergeJoin.getRight().accept(this, context);
    Type.Struct leftType = recordTypeOf(left.orElse(mergeJoin.getLeft()));
    Type.Struct rightType = recordTypeOf(right.orElse(mergeJoin.getRight()));
    Type.Struct inputType =
        recordTypeOf(left.orElse(mergeJoin.getLeft()), right.orElse(mergeJoin.getRight()));
    Optional<List<ComparisonJoinKey>> keys =
        transformList(
            mergeJoin.getKeys(),
            context,
            (key, c) -> visitComparisonJoinKey(key, leftType, rightType, c));
    Optional<Expression> postFilter =
        inInputScope(
            inputType, () -> visitOptionalExpression(mergeJoin.getPostJoinFilter(), context));
    Optional<Expression> residual =
        inInputScope(
            inputType, () -> visitOptionalExpression(mergeJoin.getResidualExpression(), context));

    if (allEmpty(left, right, keys, postFilter, residual)) {
      return Optional.empty();
    }
    return Optional.of(
        MergeJoin.builder()
            .from(mergeJoin)
            .left(left.orElse(mergeJoin.getLeft()))
            .right(right.orElse(mergeJoin.getRight()))
            .keys(keys.orElse(mergeJoin.getKeys()))
            .postJoinFilter(or(postFilter, mergeJoin::getPostJoinFilter))
            .residualExpression(or(residual, mergeJoin::getResidualExpression))
            .build());
  }

  @Override
  public Optional<Rel> visit(NestedLoopJoin nestedLoopJoin, EmptyVisitationContext context)
      throws E {
    Optional<Rel> left = nestedLoopJoin.getLeft().accept(this, context);
    Optional<Rel> right = nestedLoopJoin.getRight().accept(this, context);
    Optional<Expression> condition =
        inInputScope(
            recordTypeOf(
                left.orElse(nestedLoopJoin.getLeft()), right.orElse(nestedLoopJoin.getRight())),
            () -> nestedLoopJoin.getCondition().accept(getExpressionCopyOnWriteVisitor(), context));

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
    Optional<Rel> input = consistentPartitionWindow.getInput().accept(this, context);
    Type.Struct inputType = recordTypeOf(input.orElse(consistentPartitionWindow.getInput()));
    Optional<List<ConsistentPartitionWindow.WindowRelFunctionInvocation>> windowFunctions =
        inInputScope(
            inputType,
            () ->
                transformList(
                    consistentPartitionWindow.getWindowFunctions(),
                    context,
                    this::visitWindowRelFunction));
    Optional<List<Expression>> partitionExpressions =
        inInputScope(
            inputType,
            () -> visitExprList(consistentPartitionWindow.getPartitionExpressions(), context));
    Optional<List<Expression.SortField>> sorts =
        inInputScope(
            inputType,
            () ->
                transformList(consistentPartitionWindow.getSorts(), context, this::visitSortField));

    if (allEmpty(input, windowFunctions, partitionExpressions, sorts)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.builder()
            .from(consistentPartitionWindow)
            .input(input.orElse(consistentPartitionWindow.getInput()))
            .partitionExpressions(
                partitionExpressions.orElse(consistentPartitionWindow.getPartitionExpressions()))
            .sorts(sorts.orElse(consistentPartitionWindow.getSorts()))
            .windowFunctions(windowFunctions.orElse(consistentPartitionWindow.getWindowFunctions()))
            .build());
  }

  /**
   * Rewrites a window relation function invocation, returning a new one if anything changed.
   *
   * @param windowRelFunctionInvocation the window relation function invocation to rewrite
   * @param context the visitation context
   * @return the rewritten invocation, or empty if unchanged
   * @throws E if the visit fails
   */
  protected Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> visitWindowRelFunction(
      ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunctionInvocation,
      EmptyVisitationContext context)
      throws E {
    Optional<List<FunctionArg>> functionArgs =
        visitFunctionArguments(windowRelFunctionInvocation.arguments(), context);
    Optional<WindowBound> lowerBound =
        getExpressionCopyOnWriteVisitor()
            .visitWindowBound(windowRelFunctionInvocation.lowerBound(), context);
    Optional<WindowBound> upperBound =
        getExpressionCopyOnWriteVisitor()
            .visitWindowBound(windowRelFunctionInvocation.upperBound(), context);

    if (allEmpty(functionArgs, lowerBound, upperBound)) {
      return Optional.empty();
    }

    return Optional.of(
        ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
            .from(windowRelFunctionInvocation)
            .arguments(functionArgs.orElse(windowRelFunctionInvocation.arguments()))
            .lowerBound(lowerBound.orElse(windowRelFunctionInvocation.lowerBound()))
            .upperBound(upperBound.orElse(windowRelFunctionInvocation.upperBound()))
            .build());
  }

  // input scope tracking

  /**
   * Returns the record type that the root field references of a relation's own expressions resolve
   * against: the record types of the given inputs, concatenated in order.
   *
   * <p>Only the fields of the result are ever read, so its own nullability is not meaningful.
   *
   * @param inputs the relations the expressions are evaluated over, in field order
   * @return the combined record type
   */
  protected static Type.Struct recordTypeOf(Rel... inputs) {
    return TypeCreator.REQUIRED.struct(
        Arrays.stream(inputs).flatMap(input -> input.getRecordType().fields().stream()));
  }

  /**
   * Runs the given rewrite of a relation's own expressions with the record type that their root
   * field references resolve against, so that the cached type of a rewritten reference can be
   * re-derived from it. Inputs must be rewritten <em>before</em> calling this, and their rewritten
   * record type passed in, so that references pick up the type a replaced input emits.
   *
   * @param <T> the type of the rewrite's result
   * @param inputType the record type the expressions resolve against, or {@code null} if they do
   *     not resolve against an input record type
   * @param rewrite the expression rewrite to run
   * @return the result of the rewrite
   * @throws E if the rewrite fails
   */
  protected <T> T inInputScope(
      Type.Struct inputType, CopyOnWriteUtils.ThrowingSupplier<T, E> rewrite) throws E {
    inputTypes.add(inputType);
    try {
      return rewrite.get();
    } finally {
      inputTypes.remove(inputTypes.size() - 1);
    }
  }

  /**
   * Runs the given rewrite of expressions that do not resolve against an input record type, such as
   * the filter of a read relation, whose references resolve against the schema being read.
   *
   * @param <T> the type of the rewrite's result
   * @param rewrite the expression rewrite to run
   * @return the result of the rewrite
   * @throws E if the rewrite fails
   */
  protected <T> T outsideInputScope(CopyOnWriteUtils.ThrowingSupplier<T, E> rewrite) throws E {
    return inInputScope(null, rewrite);
  }

  /**
   * Records the scope currently being rewritten as an enclosing one for the duration of the given
   * rewrite, so that an outer reference within it resolves against the right relation.
   */
  <T> T inSubqueryScope(CopyOnWriteUtils.ThrowingSupplier<T, E> rewrite) throws E {
    outerInputTypes.add(currentInputType());
    // The relations within the subquery set their own scope as they are visited. Entering the
    // subquery with no scope keeps one of them that hosts no expressions from leaking the enclosing
    // scope into the expressions it contains.
    inputTypes.add(null);
    try {
      return rewrite.get();
    } finally {
      inputTypes.remove(inputTypes.size() - 1);
      outerInputTypes.remove(outerInputTypes.size() - 1);
    }
  }

  /**
   * Runs the given rewrite with {@code scope} registered as the record type the given rel anchor
   * exposes, so that an outer reference identified by that anchor re-derives against it. The
   * registration is undone afterwards, so an anchor is only ever in scope for the part of the tree
   * whose references it binds.
   *
   * @param <T> the type of the rewrite's result
   * @param anchor the rel anchor the references resolve to, or empty to run the rewrite unchanged
   * @param scope the record type that anchor exposes
   * @param rewrite the rewrite to run
   * @return the result of the rewrite
   * @throws E if the rewrite fails
   */
  protected <T> T inAnchorScope(
      Optional<Integer> anchor, Type.Struct scope, CopyOnWriteUtils.ThrowingSupplier<T, E> rewrite)
      throws E {
    if (!anchor.isPresent()) {
      return rewrite.get();
    }
    Type.Struct replaced = anchorScopes.put(anchor.get(), scope);
    try {
      return rewrite.get();
    } finally {
      if (replaced == null) {
        anchorScopes.remove(anchor.get());
      } else {
        anchorScopes.put(anchor.get(), replaced);
      }
    }
  }

  /**
   * Returns the record type the given rel anchor exposes to the outer references identified by it,
   * or {@code null} if that anchor is not in scope.
   */
  Type.Struct inputTypeForAnchor(int anchor) {
    return anchorScopes.get(anchor);
  }

  /**
   * Returns the record type that a field reference stepping out of {@code stepsOut} subquery levels
   * resolves against, or {@code null} if it is not known.
   */
  Type.Struct inputTypeStepsOut(int stepsOut) {
    if (stepsOut <= 0) {
      return currentInputType();
    }
    int index = outerInputTypes.size() - stepsOut;
    return index < 0 ? null : outerInputTypes.get(index);
  }

  /** Returns the record type the expressions being rewritten resolve against, if it is known. */
  private Type.Struct currentInputType() {
    return inputTypes.isEmpty() ? null : inputTypes.get(inputTypes.size() - 1);
  }

  // utilities

  /**
   * Rewrites a list of expressions, returning a new list if any expression changed.
   *
   * @param exprs the expressions to rewrite
   * @param context the visitation context
   * @return the rewritten list, or empty if unchanged
   * @throws E if the visit fails
   */
  protected Optional<List<Expression>> visitExprList(
      List<Expression> exprs, EmptyVisitationContext context) throws E {
    return transformList(exprs, context, (t, c) -> t.accept(getExpressionCopyOnWriteVisitor(), c));
  }

  /**
   * Rewrites a field reference, returning a new one if the expression it is rooted at changed or
   * its cached type no longer matches the input it resolves against.
   *
   * @param fieldReference the field reference to rewrite
   * @param context the visitation context
   * @return the rewritten field reference, or empty if unchanged
   * @throws E if the visit fails
   */
  public Optional<FieldReference> visitFieldReference(
      FieldReference fieldReference, EmptyVisitationContext context) throws E {
    return getExpressionCopyOnWriteVisitor().visitFieldReference(fieldReference, context);
  }

  /**
   * Rewrites a comparison join key, returning a new one if either side changed.
   *
   * <p>Each side is rewritten against its own input, because the field offsets of a join key are
   * relative to the side of the join they select from — unlike those of a join condition or
   * post-join filter, which are relative to the two inputs combined.
   *
   * @param key the comparison join key to rewrite
   * @param leftType the record type the key's left side selects from
   * @param rightType the record type the key's right side selects from
   * @param context the visitation context
   * @return the rewritten comparison join key, or empty if unchanged
   * @throws E if the visit fails
   */
  public Optional<ComparisonJoinKey> visitComparisonJoinKey(
      ComparisonJoinKey key,
      Type.Struct leftType,
      Type.Struct rightType,
      EmptyVisitationContext context)
      throws E {
    Optional<FieldReference> left =
        inInputScope(leftType, () -> visitFieldReference(key.getLeft(), context));
    Optional<FieldReference> right =
        inInputScope(rightType, () -> visitFieldReference(key.getRight(), context));
    if (allEmpty(left, right)) {
      return Optional.empty();
    }
    return Optional.of(
        ComparisonJoinKey.builder()
            .from(key)
            .left(left.orElse(key.getLeft()))
            .right(right.orElse(key.getRight()))
            .build());
  }

  /**
   * Rewrites a list of function arguments, returning a new list if any argument changed.
   *
   * @param funcArgs the function arguments to rewrite
   * @param context the visitation context
   * @return the rewritten list, or empty if unchanged
   * @throws E if the visit fails
   */
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

  /**
   * Rewrites a sort field, returning a new one if its expression changed.
   *
   * @param sortField the sort field to rewrite
   * @param context the visitation context
   * @return the rewritten sort field, or empty if unchanged
   * @throws E if the visit fails
   */
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
