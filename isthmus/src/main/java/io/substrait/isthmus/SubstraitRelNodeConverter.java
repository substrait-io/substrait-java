package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortDirection;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.AbstractUpdate;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter
    extends AbstractRelVisitor<RelNode, SubstraitRelNodeConverter.Context, RuntimeException> {

  protected final RelDataTypeFactory typeFactory;

  protected final ScalarFunctionConverter scalarFunctionConverter;
  protected final AggregateFunctionConverter aggregateFunctionConverter;
  protected final ExpressionRexConverter expressionRexConverter;

  protected final RelBuilder relBuilder;
  protected final RexBuilder rexBuilder;
  private final TypeConverter typeConverter;

  public SubstraitRelNodeConverter(
      final SimpleExtension.ExtensionCollection extensions,
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder) {
    this(
        typeFactory,
        relBuilder,
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory),
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
  }

  public SubstraitRelNodeConverter(
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder,
      final ScalarFunctionConverter scalarFunctionConverter,
      final AggregateFunctionConverter aggregateFunctionConverter,
      final WindowFunctionConverter windowFunctionConverter,
      final TypeConverter typeConverter) {
    this(
        typeFactory,
        relBuilder,
        scalarFunctionConverter,
        aggregateFunctionConverter,
        windowFunctionConverter,
        typeConverter,
        new ExpressionRexConverter(
            typeFactory, scalarFunctionConverter, windowFunctionConverter, typeConverter));
  }

  public SubstraitRelNodeConverter(
      final RelDataTypeFactory typeFactory,
      final RelBuilder relBuilder,
      final ScalarFunctionConverter scalarFunctionConverter,
      final AggregateFunctionConverter aggregateFunctionConverter,
      final WindowFunctionConverter windowFunctionConverter,
      final TypeConverter typeConverter,
      final ExpressionRexConverter expressionRexConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.relBuilder = relBuilder;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
    this.aggregateFunctionConverter = aggregateFunctionConverter;
    this.expressionRexConverter = expressionRexConverter;
    this.expressionRexConverter.setRelNodeConverter(this);
  }

  public static RelNode convert(
      final Rel relRoot,
      final RelOptCluster relOptCluster,
      final Prepare.CatalogReader catalogReader,
      final SqlParser.Config parserConfig) {
    final RelBuilder relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    return relRoot.accept(
        new SubstraitRelNodeConverter(
            EXTENSION_COLLECTION, relOptCluster.getTypeFactory(), relBuilder),
        Context.newContext());
  }

  @Override
  public RelNode visit(final Filter filter, final Context context) throws RuntimeException {
    final RelNode input = filter.getInput().accept(this, context);
    context.pushOuterRowType(input.getRowType());
    final RexNode filterCondition = filter.getCondition().accept(expressionRexConverter, context);
    final RelNode node =
        relBuilder.push(input).filter(context.popCorrelationIds(), filterCondition).build();
    context.popOuterRowType();
    return applyRemap(node, filter.getRemap());
  }

  @Override
  public RelNode visit(final NamedScan namedScan, final Context context) throws RuntimeException {
    final RelNode node = relBuilder.scan(namedScan.getNames()).build();
    return applyRemap(node, namedScan.getRemap());
  }

  @Override
  public RelNode visit(final LocalFiles localFiles, final Context context) throws RuntimeException {
    return visitFallback(localFiles, context);
  }

  @Override
  public RelNode visit(final EmptyScan emptyScan, final Context context) throws RuntimeException {
    final RelDataType rowType =
        typeConverter.toCalcite(relBuilder.getTypeFactory(), emptyScan.getInitialSchema().struct());
    final RelNode node = LogicalValues.create(relBuilder.getCluster(), rowType, ImmutableList.of());
    return applyRemap(node, emptyScan.getRemap());
  }

  @Override
  public RelNode visit(final Project project, final Context context) throws RuntimeException {
    final RelNode child = project.getInput().accept(this, context);
    context.pushOuterRowType(child.getRowType());

    final Stream<RexNode> directOutputs =
        IntStream.range(0, child.getRowType().getFieldCount())
            .mapToObj(fieldIndex -> rexBuilder.makeInputRef(child, fieldIndex));

    final Stream<RexNode> exprs =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter, context));

    final List<RexNode> rexExprs =
        Stream.concat(directOutputs, exprs).collect(java.util.stream.Collectors.toList());

    final RelNode node =
        relBuilder
            .push(child)
            .project(rexExprs, List.of(), false, context.popCorrelationIds())
            .build();
    context.popOuterRowType();
    return applyRemap(node, project.getRemap());
  }

  @Override
  public RelNode visit(final Cross cross, final Context context) throws RuntimeException {
    final RelNode left = cross.getLeft().accept(this, context);
    final RelNode right = cross.getRight().accept(this, context);
    // Calcite represents CROSS JOIN as the equivalent INNER JOIN with true condition
    final RelNode node =
        relBuilder.push(left).push(right).join(JoinRelType.INNER, relBuilder.literal(true)).build();
    return applyRemap(node, cross.getRemap());
  }

  @Override
  public RelNode visit(final Join join, final Context context) throws RuntimeException {
    final RelNode left = join.getLeft().accept(this, context);
    final RelNode right = join.getRight().accept(this, context);
    context.pushOuterRowType(left.getRowType(), right.getRowType());
    final RexNode condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter, context))
            .orElse(relBuilder.literal(true));
    final JoinRelType joinType = asJoinRelType(join);
    final RelNode node =
        relBuilder
            .push(left)
            .push(right)
            .join(joinType, condition, context.popCorrelationIds())
            .build();
    context.popOuterRowType();
    return applyRemap(node, join.getRemap());
  }

  private JoinRelType asJoinRelType(final Join join) {
    final Join.JoinType type = join.getJoinType();

    if (type == JoinType.INNER) {
      return JoinRelType.INNER;
    }
    if (type == JoinType.LEFT) {
      return JoinRelType.LEFT;
    }
    if (type == JoinType.RIGHT) {
      return JoinRelType.RIGHT;
    }
    if (type == JoinType.OUTER) {
      return JoinRelType.FULL;
    }
    if (type == JoinType.SEMI) {
      return JoinRelType.SEMI;
    }
    if (type == JoinType.ANTI) {
      return JoinRelType.ANTI;
    }
    if (type == JoinType.LEFT_SEMI) {
      return JoinRelType.SEMI;
    }
    if (type == JoinType.LEFT_ANTI) {
      return JoinRelType.ANTI;
    }
    if (type == JoinType.UNKNOWN) {
      throw new UnsupportedOperationException("Unknown join type is not supported");
    }

    throw new UnsupportedOperationException("Unsupported join type: " + join.getJoinType().name());
  }

  @Override
  public RelNode visit(final Set set, final Context context) throws RuntimeException {
    set.getInputs()
        .forEach(
            input -> {
              relBuilder.push(input.accept(this, context));
            });
    // TODO: MINUS_MULTISET and INTERSECTION_PRIMARY mappings are set to be removed as they do not
    //   correspond to the Calcite relations they are associated with. They are retained for now
    //   to enable users to migrate off of them.
    //   See:  https://github.com/substrait-io/substrait-java/issues/303
    final RelBuilder builder = getRelBuilder(set);
    final RelNode node = builder.build();
    return applyRemap(node, set.getRemap());
  }

  private RelBuilder getRelBuilder(final Set set) {
    final int numInputs = set.getInputs().size();

    switch (set.getSetOp()) {
      case MINUS_PRIMARY:
        return relBuilder.minus(false, numInputs);
      case MINUS_PRIMARY_ALL:
      case MINUS_MULTISET:
        return relBuilder.minus(true, numInputs);
      case INTERSECTION_PRIMARY:
      case INTERSECTION_MULTISET:
        return relBuilder.intersect(false, numInputs);
      case INTERSECTION_MULTISET_ALL:
        return relBuilder.intersect(true, numInputs);
      case UNION_DISTINCT:
        return relBuilder.union(false, numInputs);
      case UNION_ALL:
        return relBuilder.union(true, numInputs);
      case UNKNOWN:
        throw new UnsupportedOperationException("Unknown set operation is not supported");
      default:
        throw new UnsupportedOperationException("Unsupported set operation: " + set.getSetOp());
    }
  }

  @Override
  public RelNode visit(Aggregate aggregate, final Context context) throws RuntimeException {
    if (!PreCalciteAggregateValidator.isValidCalciteAggregate(aggregate)) {
      aggregate =
          PreCalciteAggregateValidator.PreCalciteAggregateTransformer
              .transformToValidCalciteAggregate(aggregate);
    }

    final RelNode child = aggregate.getInput().accept(this, context);
    final List<List<RexNode>> groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter, context))
                        .collect(java.util.stream.Collectors.toList()))
            .collect(java.util.stream.Collectors.toList());
    final List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs, groupExprLists);

    final List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream()
            .map(measure -> fromMeasure(measure, context))
            .collect(java.util.stream.Collectors.toList());

    Optional<Remap> remap = aggregate.getRemap();
    final int lastFieldIndex = groupExprs.size() + aggregateCalls.size();

    // map grouping set index if it is not removed via remap
    final boolean emitDirect = remap.isEmpty();
    final boolean groupingSetIndexGetsRemapped =
        remap.map(r -> r.indices().contains(lastFieldIndex)).orElse(false);
    if (aggregate.getGroupings().size() > 1 && (emitDirect || groupingSetIndexGetsRemapped)) {
      aggregateCalls.add(
          AggregateCall.create(
              SqlStdOperatorTable.GROUP_ID,
              false,
              false,
              false,
              Collections.emptyList(),
              Collections.emptyList(),
              -1,
              null,
              RelCollations.EMPTY,
              typeConverter.toCalcite(typeFactory, TypeCreator.REQUIRED.I64),
              null));
      final int groupingCallIndex = aggregateCalls.size() - 1;
      if (groupingSetIndexGetsRemapped) {
        final List<Integer> remapList = new LinkedList<>(remap.get().indices());
        for (int i = 0; i < remapList.size(); i++) {
          if (remapList.get(i).equals(lastFieldIndex)) {
            // replace last field index with field index of the GROUP_ID() function call
            remapList.set(i, groupingCallIndex);
          }
        }
        remap = Optional.of(Remap.of(remapList));
      }
    }

    final RelNode node = relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
    return applyRemap(node, remap);
  }

  private AggregateCall fromMeasure(final Aggregate.Measure measure, final Context context) {
    final List<FunctionArg> eArgs = measure.getFunction().arguments();
    final List<RexNode> arguments =
        IntStream.range(0, measure.getFunction().arguments().size())
            .mapToObj(
                i ->
                    eArgs
                        .get(i)
                        .accept(
                            measure.getFunction().declaration(),
                            i,
                            expressionRexConverter,
                            context))
            .collect(java.util.stream.Collectors.toList());
    final Optional<SqlOperator> operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(), measure.getFunction().outputType());
    if (!operator.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    final List<Integer> argIndex = new ArrayList<>();
    for (final RexNode arg : arguments) {
      // arguments are guaranteed to be RexInputRef because of the prior call to
      // transformToValidCalciteAggregate
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    final boolean distinct =
        measure.getFunction().invocation().equals(Expression.AggregationInvocation.DISTINCT);

    final SqlAggFunction aggFunction;
    final RelDataType returnType =
        typeConverter.toCalcite(typeFactory, measure.getFunction().getType());

    if (operator.get() instanceof SqlAggFunction) {
      aggFunction = (SqlAggFunction) operator.get();
    } else {
      final String msg =
          String.format(
              "Unable to convert non-aggregate operator: %s for substrait aggregate function %s",
              operator.get(), measure.getFunction().declaration().name());
      throw new IllegalArgumentException(msg);
    }

    int filterArg = -1;
    if (measure.getPreMeasureFilter().isPresent()) {
      final RexNode filter =
          measure.getPreMeasureFilter().get().accept(expressionRexConverter, context);
      filterArg = ((RexInputRef) filter).getIndex();
    }

    RelCollation relCollation = RelCollations.EMPTY;
    if (!measure.getFunction().sort().isEmpty()) {
      relCollation =
          RelCollations.of(
              measure.getFunction().sort().stream()
                  .map(sortField -> toRelFieldCollation(sortField, context))
                  .collect(Collectors.toList()));
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        Collections.emptyList(),
        argIndex,
        filterArg,
        null,
        relCollation,
        returnType,
        null);
  }

  @Override
  public RelNode visit(final Sort sort, final Context context) throws RuntimeException {
    final RelNode child = sort.getInput().accept(this, context);
    final List<RexNode> sortExpressions =
        sort.getSortFields().stream()
            .map(sortField -> directedRexNode(sortField, context))
            .collect(Collectors.toList());
    final RelNode node = relBuilder.push(child).sort(sortExpressions).build();
    return applyRemap(node, sort.getRemap());
  }

  private RexNode directedRexNode(final Expression.SortField sortField, final Context context) {
    final Expression expression = sortField.expr();
    final RexNode rexNode = expression.accept(expressionRexConverter, context);
    final SortDirection sortDirection = sortField.direction();

    if (sortDirection == Expression.SortDirection.ASC_NULLS_FIRST) {
      return relBuilder.nullsFirst(rexNode);
    }
    if (sortDirection == Expression.SortDirection.ASC_NULLS_LAST) {
      return relBuilder.nullsLast(rexNode);
    }
    if (sortDirection == Expression.SortDirection.DESC_NULLS_FIRST) {
      return relBuilder.nullsFirst(relBuilder.desc(rexNode));
    }
    if (sortDirection == Expression.SortDirection.DESC_NULLS_LAST) {
      return relBuilder.nullsLast(relBuilder.desc(rexNode));
    }
    if (sortDirection == Expression.SortDirection.CLUSTERED) {
      throw new UnsupportedOperationException(
          String.format("Unexpected Expression.SortDirection: Clustered!"));
    }

    throw new IllegalArgumentException("Unsupported sort direction: " + sortDirection);
  }

  @Override
  public RelNode visit(final Fetch fetch, final Context context) throws RuntimeException {
    final RelNode child = fetch.getInput().accept(this, context);
    final OptionalLong optCount = fetch.getCount();
    final long count = optCount.orElse(-1L);
    final long offset = fetch.getOffset();
    if (offset > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("offset is overflowed as an integer: %d", offset));
    }
    if (count > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("count is overflowed as an integer: %d", count));
    }
    final RelNode node = relBuilder.push(child).limit((int) offset, (int) count).build();
    return applyRemap(node, fetch.getRemap());
  }

  private RelFieldCollation toRelFieldCollation(
      final Expression.SortField sortField, final Context context) {
    final Expression expression = sortField.expr();
    final RexNode rex = expression.accept(expressionRexConverter, context);
    final SortDirection sortDirection = sortField.direction();
    final RexSlot rexSlot = (RexSlot) rex;
    final int fieldIndex = rexSlot.getIndex();

    final RelFieldCollation.Direction fieldDirection;
    final RelFieldCollation.NullDirection nullDirection;

    if (sortDirection == SortDirection.ASC_NULLS_FIRST) {
      fieldDirection = RelFieldCollation.Direction.ASCENDING;
      nullDirection = RelFieldCollation.NullDirection.FIRST;
    } else if (sortDirection == SortDirection.ASC_NULLS_LAST) {
      fieldDirection = RelFieldCollation.Direction.ASCENDING;
      nullDirection = RelFieldCollation.NullDirection.LAST;
    } else if (sortDirection == SortDirection.DESC_NULLS_FIRST) {
      nullDirection = RelFieldCollation.NullDirection.FIRST;
      fieldDirection = RelFieldCollation.Direction.DESCENDING;
    } else if (sortDirection == SortDirection.DESC_NULLS_LAST) {
      nullDirection = RelFieldCollation.NullDirection.LAST;
      fieldDirection = RelFieldCollation.Direction.DESCENDING;
    } else if (sortDirection == SortDirection.CLUSTERED) {
      fieldDirection = RelFieldCollation.Direction.CLUSTERED;
      nullDirection = RelFieldCollation.NullDirection.UNSPECIFIED;
    } else {
      throw new UnsupportedOperationException(
          String.format("Unexpected Expression.SortDirection enum: %s !", sortDirection));
    }

    return new RelFieldCollation(fieldIndex, fieldDirection, nullDirection);
  }

  @Override
  public RelNode visit(final NamedUpdate update, final Context context) {
    relBuilder.scan(update.getNames());
    final RexNode condition = update.getCondition().accept(expressionRexConverter, context);
    relBuilder.filter(condition);
    final RelNode inputForModify = relBuilder.build();

    final NamedStruct tableSchema = update.getTableSchema();
    final List<String> fieldNames = tableSchema.names();

    final List<String> updateColumnList = new ArrayList<>();
    final List<RexNode> sourceExpressionList = new ArrayList<>();

    for (final AbstractUpdate.TransformExpression transform : update.getTransformations()) {

      updateColumnList.add(fieldNames.get(transform.getColumnTarget()));
      sourceExpressionList.add(
          transform.getTransformation().accept(expressionRexConverter, context));
    }

    assert relBuilder.getRelOptSchema() != null;
    final RelOptTable table = relBuilder.getRelOptSchema().getTableForMember(update.getNames());

    if (table == null) {
      throw new IllegalStateException("Table not found in Calcite catalog: " + update.getNames());
    }
    final Prepare.CatalogReader catalogReader = (Prepare.CatalogReader) table.getRelOptSchema();

    assert catalogReader != null;
    return LogicalTableModify.create(
        table,
        catalogReader,
        inputForModify,
        TableModify.Operation.UPDATE,
        updateColumnList,
        sourceExpressionList,
        false);
  }

  @Override
  public RelNode visit(final ScatterExchange exchange, final Context context)
      throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(final SingleBucketExchange exchange, final Context context)
      throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(final MultiBucketExchange exchange, final Context context)
      throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(final RoundRobinExchange exchange, final Context context)
      throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(final BroadcastExchange exchange, final Context context)
      throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(final NamedDdl namedDdl, final Context context) {
    if (namedDdl.getOperation() != AbstractDdlRel.DdlOp.CREATE
        || namedDdl.getObject() != AbstractDdlRel.DdlObject.VIEW) {
      throw new UnsupportedOperationException(
          String.format(
              "Can only handle NamedDdl with (%s, %s), given (%s, %s)",
              AbstractDdlRel.DdlOp.CREATE,
              AbstractDdlRel.DdlObject.VIEW,
              namedDdl.getOperation(),
              namedDdl.getObject()));
    }

    if (namedDdl.getViewDefinition().isEmpty()) {
      throw new IllegalArgumentException("NamedDdl view definition must be set");
    }

    final Rel viewDefinition = namedDdl.getViewDefinition().get();
    final RelNode relNode = viewDefinition.accept(this, context);
    return new CreateView(namedDdl.getNames(), relNode);
  }

  @Override
  public RelNode visit(final VirtualTableScan virtualTableScan, final Context context) {
    final RelDataType typeInfoOnly =
        typeConverter.toCalcite(typeFactory, virtualTableScan.getInitialSchema().struct());

    final List<String> correctFieldNames = virtualTableScan.getInitialSchema().names();

    final List<RelDataType> fieldTypes =
        typeInfoOnly.getFieldList().stream()
            .map(RelDataTypeField::getType)
            .collect(Collectors.toList());

    final RelDataType rowTypeWithNames =
        typeFactory.createStructType(fieldTypes, correctFieldNames);

    final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>();
    for (final Expression.StructLiteral row : virtualTableScan.getRows()) {
      final List<RexLiteral> rexRow = new ArrayList<>();
      for (final Expression.Literal literal : row.fields()) {
        final RexNode rexNode = literal.accept(expressionRexConverter, context);
        if (rexNode instanceof RexLiteral) {
          final RexLiteral rexLiteral = (RexLiteral) rexNode;
          rexRow.add(rexLiteral);
        } else {
          throw new UnsupportedOperationException(
              "VirtualTableScan only supports literal values, found: "
                  + rexNode.getClass().getName());
        }
      }
      tuples.add(ImmutableList.copyOf(rexRow));
    }

    return LogicalValues.create(
        relBuilder.getCluster(), rowTypeWithNames, ImmutableList.copyOf(tuples));
  }

  private RelNode handleCreateTableAs(final NamedWrite namedWrite, final Context context) {
    if (namedWrite.getCreateMode() != AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS
        || namedWrite.getOutputMode() != AbstractWriteRel.OutputMode.NO_OUTPUT) {
      throw new UnsupportedOperationException(
          String.format(
              "Can only handle CTAS NamedWrite with (%s, %s), given (%s, %s)",
              AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS,
              AbstractWriteRel.OutputMode.NO_OUTPUT,
              namedWrite.getCreateMode(),
              namedWrite.getOutputMode()));
    }

    final Rel input = namedWrite.getInput();
    final RelNode relNode = input.accept(this, context);
    return new CreateTable(namedWrite.getNames(), relNode);
  }

  @Override
  public RelNode visit(final NamedWrite write, final Context context) {
    final RelNode input = write.getInput().accept(this, context);
    assert relBuilder.getRelOptSchema() != null;
    final RelOptTable targetTable =
        relBuilder.getRelOptSchema().getTableForMember(write.getNames());

    final TableModify.Operation operation;
    switch (write.getOperation()) {
      case INSERT:
        operation = TableModify.Operation.INSERT;
        break;
      case DELETE:
        operation = TableModify.Operation.DELETE;
        break;
      case CTAS:
        return handleCreateTableAs(write, context);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "NamedWrite with WriteOp %s cannot be converted to a Calcite RelNode. Consider using a more specific Rel (e.g NamedUpdate)",
                write.getOperation()));
    }

    // checked by validation
    assert targetTable != null;

    return LogicalTableModify.create(
        targetTable,
        (Prepare.CatalogReader) relBuilder.getRelOptSchema(),
        input,
        operation,
        null,
        null,
        false);
  }

  @Override
  public RelNode visitFallback(final Rel rel, final Context context) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  protected RelNode applyRemap(final RelNode relNode, final Optional<Rel.Remap> remap) {
    if (remap.isPresent()) {
      return applyRemap(relNode, remap.get());
    }
    return relNode;
  }

  private RelNode applyRemap(final RelNode relNode, final Rel.Remap remap) {
    final RelDataType rowType = relNode.getRowType();
    final List<String> fieldNames = rowType.getFieldNames();
    final List<RexNode> rexList =
        remap.indices().stream()
            .map(
                index -> {
                  final RelDataTypeField t = rowType.getField(fieldNames.get(index), true, false);
                  return new RexInputRef(index, t.getValue());
                })
            .collect(java.util.stream.Collectors.toList());
    return relBuilder.push(relNode).project(rexList).build();
  }

  /** A shared context for the Substrait to RelNode conversion. */
  public static class Context implements VisitationContext {
    protected final Stack<RangeMap<Integer, RelDataType>> outerRowTypes = new Stack<>();

    protected final Stack<java.util.Set<CorrelationId>> correlationIds = new Stack<>();

    private int subqueryDepth;

    /**
     * Creates a new {@link Context} instance.
     *
     * @return the new {@link Context} instance
     */
    public static Context newContext() {
      return new Context();
    }

    /**
     * Adds the outer row types to the top of the stack of outer row types.
     *
     * <p>Row types are stored as a {@link RangeMap} with field indices as keys and the {@link
     * RelDataType} row type containing the field at the field index by continuously numbering the
     * field indices from 0 across all provided row types in the order the row types are passed as
     * arguments.
     *
     * @param inputs the row types to add
     */
    public void pushOuterRowType(final RelDataType... inputs) {
      final RangeMap<Integer, RelDataType> fieldRangeMap = TreeRangeMap.create();
      int begin = 0;
      for (final RelDataType parent : inputs) {
        final int end = begin + parent.getFieldCount();
        final Range<Integer> range = Range.closedOpen(begin, end);
        fieldRangeMap.put(range, parent);
        begin = end;
      }

      outerRowTypes.push(fieldRangeMap);
      this.correlationIds.push(new HashSet<>());
    }

    public void popOuterRowType() {
      outerRowTypes.pop();
    }

    /**
     * Returns the outer row type {@link RangeMap} walking up the given steps from the current
     * subquery depth.
     *
     * @param stepsOut number of steps to walk up from the current subquery depth
     * @return {@link RangeMap} with field indices as keys and the {@link RelDataType} row type
     *     containing the field at the field index
     */
    public RangeMap<Integer, RelDataType> getOuterRowTypeRangeMap(final Integer stepsOut) {
      return this.outerRowTypes.get(subqueryDepth - stepsOut);
    }

    /**
     * Removes the correlation ids at the top of the stack.
     *
     * @return the correlation ids removed from the top of the stack
     */
    public java.util.Set<CorrelationId> popCorrelationIds() {
      return correlationIds.pop();
    }

    /**
     * Adds a {@link CorrelationId} to the subquery depth walking up the given steps from the
     * current subquery depth.
     *
     * @param stepsOut number of steps to walk up from the current subquery depth
     * @param correlationId the {@link CorrelationId} to add
     */
    public void addCorrelationId(final int stepsOut, final CorrelationId correlationId) {
      final int index = subqueryDepth - stepsOut;
      this.correlationIds.get(index).add(correlationId);
    }

    /** Increments the current subquery depth. */
    public void incrementSubqueryDepth() {
      this.subqueryDepth++;
    }

    /** Decrements the current subquery depth. */
    public void decrementSubqueryDepth() {
      this.subqueryDepth--;
    }
  }

  /**
   * Returns the {@link RelBuilder} of this converter.
   *
   * @return the {@link RelBuilder}
   */
  public RelBuilder getRelBuilder() {
    return relBuilder;
  }
}
