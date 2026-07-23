package io.substrait.isthmus;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortDirection;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.AbstractUpdate;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.OuterReferenceConverter;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
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
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter
    extends AbstractRelVisitor<RelNode, SubstraitRelNodeConverter.Context, RuntimeException> {

  /** Calcite type factory used to construct row and field types. */
  protected final RelDataTypeFactory typeFactory;

  /** Converter for Substrait scalar functions to Calcite operators. */
  protected final ScalarFunctionConverter scalarFunctionConverter;

  /** Converter for Substrait aggregate functions to Calcite operators. */
  protected final AggregateFunctionConverter aggregateFunctionConverter;

  /** Converts Substrait {@code Expression}s into Calcite {@code RexNode}s. */
  protected final ExpressionRexConverter expressionRexConverter;

  /** Calcite {@link RelBuilder} used to construct relational expressions during conversion. */
  protected final RelBuilder relBuilder;

  /** Calcite {@link RexBuilder} used to build Rex nodes (e.g., input refs, literals). */
  protected final RexBuilder rexBuilder;

  /** Type converter to translate between Calcite and Substrait type systems. */
  private final TypeConverter typeConverter;

  /**
   * Creates a new SubstraitRelNodeConverter with the specified extensions, type factory, and
   * relation builder.
   *
   * @param extensions the Substrait extension collection
   * @param typeFactory the Calcite type factory
   * @param relBuilder the Calcite relation builder
   * @deprecated Use {@link #SubstraitRelNodeConverter(RelBuilder, ConverterProvider)} instead
   */
  @Deprecated
  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelDataTypeFactory typeFactory,
      RelBuilder relBuilder) {
    this(relBuilder, new ConverterProvider(extensions, typeFactory));
  }

  /**
   * Creates a new SubstraitRelNodeConverter with the specified relation builder and converter
   * provider.
   *
   * @param relBuilder the Calcite relation builder
   * @param converterProvider the converter provider containing configuration and converters
   */
  public SubstraitRelNodeConverter(RelBuilder relBuilder, ConverterProvider converterProvider) {
    this.typeFactory = converterProvider.getTypeFactory();
    this.typeConverter = converterProvider.getTypeConverter();
    this.relBuilder = relBuilder;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = converterProvider.getScalarFunctionConverter();
    this.aggregateFunctionConverter = converterProvider.getAggregateFunctionConverter();
    this.expressionRexConverter = converterProvider.getExpressionRexConverter(this);
  }

  /**
   * Converts a Substrait {@link Rel} plan to a Calcite {@link RelNode}.
   *
   * @param relRoot the root Substrait relation to convert
   * @param catalogReader the Calcite catalog reader for schema resolution
   * @param converterProvider the converter provider containing configuration and converters
   * @return the converted Calcite {@link RelNode}
   */
  public static RelNode convert(
      Rel relRoot, Prepare.CatalogReader catalogReader, ConverterProvider converterProvider) {
    RelBuilder relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(converterProvider.getSqlParserConfig())
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .typeSystem(converterProvider.getTypeSystem())
                .programs()
                .build());
    // Normalize any offset-based outer references (steps_out) to the id-based form (rel_anchor /
    // rel_reference) so the conversion below resolves correlations purely by anchor. Plans that are
    // already id-based are left unchanged.
    return OuterReferenceConverter.toIdBased(relRoot)
        .accept(converterProvider.getSubstraitRelNodeConverter(relBuilder), Context.newContext());
  }

  @Override
  public RelNode visit(Filter filter, Context context) throws RuntimeException {
    RelNode input = filter.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(filter.getInput().getRelAnchor(), input.getRowType()));
    RexNode filterCondition = filter.getCondition().accept(expressionRexConverter, context);
    RelNode node = relBuilder.push(input).filter(context.exitScope(), filterCondition).build();
    return applyRemap(node, filter.getRemap());
  }

  @Override
  public RelNode visit(NamedScan namedScan, Context context) throws RuntimeException {
    RelNode node = relBuilder.scan(namedScan.getNames()).build();
    return applyRemap(node, namedScan.getRemap());
  }

  @Override
  public RelNode visit(LocalFiles localFiles, Context context) throws RuntimeException {
    return visitFallback(localFiles, context);
  }

  @Override
  public RelNode visit(Project project, Context context) throws RuntimeException {
    RelNode child = project.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(project.getInput().getRelAnchor(), child.getRowType()));

    Stream<RexNode> directOutputs =
        IntStream.range(0, child.getRowType().getFieldCount())
            .mapToObj(fieldIndex -> rexBuilder.makeInputRef(child, fieldIndex));

    Stream<RexNode> exprs =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter, context));

    List<RexNode> rexExprs =
        Stream.concat(directOutputs, exprs).collect(java.util.stream.Collectors.toList());

    RelNode node =
        relBuilder.push(child).project(rexExprs, List.of(), false, context.exitScope()).build();
    return applyRemap(node, project.getRemap());
  }

  @Override
  public RelNode visit(Cross cross, Context context) throws RuntimeException {
    RelNode left = cross.getLeft().accept(this, context);
    RelNode right = cross.getRight().accept(this, context);
    // Calcite represents CROSS JOIN as the equivalent INNER JOIN with true condition
    RelNode node =
        relBuilder.push(left).push(right).join(JoinRelType.INNER, relBuilder.literal(true)).build();
    return applyRemap(node, cross.getRemap());
  }

  @Override
  public RelNode visit(Join join, Context context) throws RuntimeException {
    RelNode left = join.getLeft().accept(this, context);
    RelNode right = join.getRight().accept(this, context);
    context.enterScope(
        AnchoredInput.of(join.getLeft().getRelAnchor(), left.getRowType()),
        AnchoredInput.of(join.getRight().getRelAnchor(), right.getRowType()));
    RexNode condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter, context))
            .orElse(relBuilder.literal(true));
    JoinRelType joinType = asJoinRelType(join);
    RelNode node =
        relBuilder.push(left).push(right).join(joinType, condition, context.exitScope()).build();
    return applyRemap(node, join.getRemap());
  }

  private JoinRelType asJoinRelType(Join join) {
    Join.JoinType type = join.getJoinType();

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
  public RelNode visit(Set set, Context context) throws RuntimeException {
    set.getInputs()
        .forEach(
            input -> {
              relBuilder.push(input.accept(this, context));
            });
    RelBuilder builder = getRelBuilder(set);
    RelNode node = builder.build();
    return applyRemap(node, set.getRemap());
  }

  private RelBuilder getRelBuilder(Set set) {
    int numInputs = set.getInputs().size();

    switch (set.getSetOp()) {
      case MINUS_PRIMARY:
        return relBuilder.minus(false, numInputs);
      case MINUS_PRIMARY_ALL:
        return relBuilder.minus(true, numInputs);
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
  public RelNode visit(Aggregate aggregate, Context context) throws RuntimeException {
    if (!PreCalciteAggregateValidator.isValidCalciteAggregate(aggregate)) {
      aggregate =
          PreCalciteAggregateValidator.PreCalciteAggregateTransformer
              .transformToValidCalciteAggregate(aggregate);
    }

    RelNode child = aggregate.getInput().accept(this, context);
    List<List<RexNode>> groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter, context))
                        .collect(java.util.stream.Collectors.toList()))
            .collect(java.util.stream.Collectors.toList());
    List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs, groupExprLists);

    List<AggregateCall> aggregateCalls =
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
        List<Integer> remapList = new LinkedList<>(remap.get().indices());
        for (int i = 0; i < remapList.size(); i++) {
          if (remapList.get(i).equals(lastFieldIndex)) {
            // replace last field index with field index of the GROUP_ID() function call
            remapList.set(i, groupingCallIndex);
          }
        }
        remap = Optional.of(Remap.of(remapList));
      }
    }

    RelNode node = relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
    return applyRemap(node, remap);
  }

  private AggregateCall fromMeasure(Aggregate.Measure measure, Context context) {
    List<FunctionArg> eArgs = measure.getFunction().arguments();
    // Only value (Expression) arguments map to Calcite aggregate operands. Enum arguments such as
    // the std_dev/variance "distribution" are used to disambiguate the operator, not as operands.
    List<RexNode> arguments =
        IntStream.range(0, eArgs.size())
            .filter(i -> eArgs.get(i) instanceof Expression)
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
    Optional<SqlOperator> operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(),
            measure.getFunction().outputType(),
            measure.getFunction().arguments());
    if (!operator.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    List<Integer> argIndex = new ArrayList<>();
    for (RexNode arg : arguments) {
      // arguments are guaranteed to be RexInputRef because of the prior call to
      // transformToValidCalciteAggregate
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    boolean distinct =
        measure.getFunction().invocation().equals(Expression.AggregationInvocation.DISTINCT);

    SqlAggFunction aggFunction;
    RelDataType returnType = typeConverter.toCalcite(typeFactory, measure.getFunction().getType());

    if (operator.get() instanceof SqlAggFunction) {
      aggFunction = (SqlAggFunction) operator.get();
    } else {
      String msg =
          String.format(
              "Unable to convert non-aggregate operator: %s for substrait aggregate function %s",
              operator.get(), measure.getFunction().declaration().name());
      throw new IllegalArgumentException(msg);
    }

    int filterArg = -1;
    if (measure.getPreMeasureFilter().isPresent()) {
      RexNode filter = measure.getPreMeasureFilter().get().accept(expressionRexConverter, context);
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
  public RelNode visit(Sort sort, Context context) throws RuntimeException {
    RelNode child = sort.getInput().accept(this, context);
    List<RexNode> sortExpressions =
        sort.getSortFields().stream()
            .map(sortField -> directedRexNode(sortField, context))
            .collect(Collectors.toList());
    RelNode node = relBuilder.push(child).sort(sortExpressions).build();
    return applyRemap(node, sort.getRemap());
  }

  private RexNode directedRexNode(Expression.SortField sortField, Context context) {
    Expression expression = sortField.expr();
    RexNode rexNode = expression.accept(expressionRexConverter, context);
    SortDirection sortDirection = sortField.direction();

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
  public RelNode visit(Fetch fetch, Context context) throws RuntimeException {
    RelNode child = fetch.getInput().accept(this, context);
    OptionalLong optCount = fetch.getCount();
    long count = optCount.orElse(-1L);
    long offset = fetch.getOffset();
    if (offset > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("offset is overflowed as an integer: %d", offset));
    }
    if (count > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("count is overflowed as an integer: %d", count));
    }
    RelNode node = relBuilder.push(child).limit((int) offset, (int) count).build();
    return applyRemap(node, fetch.getRemap());
  }

  private RelFieldCollation toRelFieldCollation(Expression.SortField sortField, Context context) {
    Expression expression = sortField.expr();
    RexNode rex = expression.accept(expressionRexConverter, context);
    SortDirection sortDirection = sortField.direction();
    RexSlot rexSlot = (RexSlot) rex;
    int fieldIndex = rexSlot.getIndex();

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
  public RelNode visit(NamedUpdate update, Context context) {
    relBuilder.scan(update.getNames());
    RexNode condition = update.getCondition().accept(expressionRexConverter, context);
    relBuilder.filter(condition);
    RelNode inputForModify = relBuilder.build();

    NamedStruct tableSchema = update.getTableSchema();
    List<String> fieldNames = tableSchema.names();

    List<String> updateColumnList = new ArrayList<>();
    List<RexNode> sourceExpressionList = new ArrayList<>();

    for (AbstractUpdate.TransformExpression transform : update.getTransformations()) {

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
  public RelNode visit(ScatterExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(SingleBucketExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(MultiBucketExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(RoundRobinExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(BroadcastExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(NamedDdl namedDdl, Context context) {
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

    Rel viewDefinition = namedDdl.getViewDefinition().get();
    RelNode relNode = viewDefinition.accept(this, context);
    return new CreateView(namedDdl.getNames(), relNode);
  }

  @Override
  public RelNode visit(VirtualTableScan virtualTableScan, Context context) {
    final RelDataType rowType =
        typeConverter.toCalcite(typeFactory, virtualTableScan.getInitialSchema().struct());

    final List<String> correctFieldNames = virtualTableScan.getInitialSchema().names();

    final List<RelDataType> fieldTypes =
        rowType.getFieldList().stream().map(RelDataTypeField::getType).collect(Collectors.toList());

    final RelDataType rowTypeWithNames =
        typeFactory.createStructType(fieldTypes, correctFieldNames);

    // When all expression in the VirtualTable are literals, we can encode it in Calcite as a
    // standard LogicalValues relation.
    boolean allLiterals =
        virtualTableScan.getRows().stream()
            .allMatch(row -> row.fields().stream().allMatch(e -> e instanceof Expression.Literal));
    if (allLiterals) {
      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();
      for (final Expression.NestedStruct rowExpr : virtualTableScan.getRows()) {
        ImmutableList.Builder<RexLiteral> tupleBuilder = ImmutableList.builder();
        for (Expression expr : rowExpr.fields()) {
          final Expression.Literal literal = (Expression.Literal) expr;
          final RexLiteral rexNode = (RexLiteral) literal.accept(expressionRexConverter, context);
          tupleBuilder.add(rexNode);
        }
        tuplesBuilder.add(tupleBuilder.build());
      }
      return LogicalValues.create(relBuilder.getCluster(), rowTypeWithNames, tuplesBuilder.build());
    } else {
      // When a VirtualTable contains non-literal expressions, they cannot be put directly into a
      // LogicalValues relation. Instead, we create a LogicalProject for each row to compute its
      // values, and combine them together using a LogicalUnion. For example the following:
      //
      //   VirtualTable
      //     (e1, e2)
      //     (e3, e4)
      //
      //  Becomes:
      //
      //   LogicalUnion(all=[true])
      //     LogicalProject(exprs=[e1, e2])
      //       <Empty Row>
      //     LogicalProject(exprs=[e3, e4])
      //       <Empty Row>
      //

      RelDataType emptyRowType = typeFactory.createStructType(List.of(), List.of());
      ImmutableList<ImmutableList<RexLiteral>> emptyRowValue = ImmutableList.of(ImmutableList.of());

      List<RelNode> projects = new ArrayList<>();
      for (final Expression.NestedStruct rowExpr : virtualTableScan.getRows()) {
        List<RexNode> rexRow = new ArrayList<>();
        for (Expression field : rowExpr.fields()) {
          rexRow.add(field.accept(expressionRexConverter, context));
        }
        RelNode values = LogicalValues.create(relBuilder.getCluster(), emptyRowType, emptyRowValue);
        RelNode project =
            LogicalProject.create(
                values, Collections.emptyList(), rexRow, rowType, Collections.emptySet());
        projects.add(project);
      }
      RelNode union = LogicalUnion.create(projects, true);

      // Apply a final LogicalProject on top to capture the field names from the VirtualTable
      List<RexNode> topProjectExprs = new ArrayList<>();
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        topProjectExprs.add(rexBuilder.makeInputRef(union, i));
      }
      return LogicalProject.create(
          union,
          Collections.emptyList(),
          topProjectExprs,
          rowTypeWithNames,
          Collections.emptySet());
    }
  }

  private RelNode handleCreateTableAs(NamedWrite namedWrite, Context context) {
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

    Rel input = namedWrite.getInput();
    RelNode relNode = input.accept(this, context);
    return new CreateTable(namedWrite.getNames(), relNode);
  }

  @Override
  public RelNode visit(NamedWrite write, Context context) {
    RelNode input = write.getInput().accept(this, context);
    assert relBuilder.getRelOptSchema() != null;
    final RelOptTable targetTable =
        relBuilder.getRelOptSchema().getTableForMember(write.getNames());

    TableModify.Operation operation;
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
  public RelNode visitFallback(Rel rel, Context context) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  /**
   * Applies an optional field remap to the given node.
   *
   * <p>If {@code remap} is present, the node is projected according to the provided indices;
   * otherwise the original node is returned unchanged.
   *
   * @param relNode the node to remap
   * @param remap optional field index remap
   * @return remapped node or original node if no remap is present
   */
  protected RelNode applyRemap(RelNode relNode, Optional<Rel.Remap> remap) {
    if (remap.isPresent()) {
      return applyRemap(relNode, remap.get());
    }
    return relNode;
  }

  private RelNode applyRemap(RelNode relNode, Rel.Remap remap) {
    RelDataType rowType = relNode.getRowType();
    List<String> fieldNames = rowType.getFieldNames();
    List<RexNode> rexList =
        remap.indices().stream()
            .map(
                index -> {
                  RelDataTypeField t = rowType.getField(fieldNames.get(index), true, false);
                  return new RexInputRef(index, t.getValue());
                })
            .collect(java.util.stream.Collectors.toList());
    return relBuilder.push(relNode).project(rexList).build();
  }

  /**
   * A relational input paired with the {@code rel_anchor} it may carry, used by {@link Context}.
   */
  public static final class AnchoredInput {
    final Optional<Integer> anchor;
    final RelDataType rowType;

    private AnchoredInput(Optional<Integer> anchor, RelDataType rowType) {
      this.anchor = anchor;
      this.rowType = rowType;
    }

    /**
     * Creates an anchored input.
     *
     * @param anchor the input's {@code rel_anchor}, if any
     * @param rowType the input's Calcite row type
     * @return the anchored input
     */
    public static AnchoredInput of(Optional<Integer> anchor, RelDataType rowType) {
      return new AnchoredInput(anchor, rowType);
    }
  }

  /**
   * A shared context for the Substrait to RelNode conversion.
   *
   * <p>Correlated (outer) references are resolved by id: a relation that binds an outer reference
   * carries a {@code rel_anchor}, and each reference carries the matching {@code rel_reference}.
   * When a relational operator is converted, it enters a scope recording the anchors of its inputs;
   * a reference then resolves against the enclosing scope owning its anchor, and the {@link
   * CorrelationId} minted for that anchor is attached to the operator that owns it.
   */
  public static class Context implements VisitationContext {

    /** Stack of correlation scopes, innermost on top. */
    private final Deque<Scope> scopes = new ArrayDeque<>();

    /** Maps an in-scope {@code rel_anchor} to the scope that owns it. */
    private final Map<Integer, Scope> scopeByAnchor = new HashMap<>();

    /** Maps a {@code rel_anchor} to the single {@link CorrelationId} minted for it. */
    private final Map<Integer, CorrelationId> correlationIdByAnchor = new HashMap<>();

    /**
     * Every {@code rel_anchor} that has entered a scope. Resolution keys {@link #scopeByAnchor} and
     * {@link #correlationIdByAnchor} purely by anchor value, which is only sound if anchors are
     * unique plan-wide (as required by {@link io.substrait.relation.Rel#getRelAnchor()}). This set
     * lets {@link #enterScope} reject a plan that reuses an anchor for two distinct relations
     * rather than silently mis-resolving references.
     */
    private final java.util.Set<Integer> seenAnchors = new HashSet<>();

    /** One correlation scope per enclosing relational operator. */
    private static final class Scope {
      final Map<Integer, RelDataType> rowTypeByAnchor = new HashMap<>();
      final java.util.Set<CorrelationId> correlationIds = new HashSet<>();
    }

    /**
     * Creates a new {@link Context} instance.
     *
     * @return the new {@link Context} instance
     */
    public static Context newContext() {
      return new Context();
    }

    /**
     * Enters a correlation scope for a relational operator, recording the {@code rel_anchor} (if
     * any) carried by each of its inputs.
     *
     * @param inputs the operator's inputs paired with their anchors
     */
    public void enterScope(final AnchoredInput... inputs) {
      final Scope scope = new Scope();
      for (final AnchoredInput input : inputs) {
        if (input.anchor.isPresent()) {
          final int anchor = input.anchor.get();
          if (!seenAnchors.add(anchor)) {
            throw new UnsupportedOperationException(
                "Duplicate rel_anchor="
                    + anchor
                    + "; rel_anchors must be unique plan-wide for id-based outer references to "
                    + "resolve unambiguously");
          }
          scope.rowTypeByAnchor.put(anchor, input.rowType);
          scopeByAnchor.put(anchor, scope);
        }
      }
      scopes.push(scope);
    }

    /**
     * Exits the innermost correlation scope, returning the correlation ids to attach to the
     * operator that owns it.
     *
     * @return the correlation ids resolved against this operator's inputs
     */
    public java.util.Set<CorrelationId> exitScope() {
      final Scope scope = scopes.pop();
      for (final Integer anchor : scope.rowTypeByAnchor.keySet()) {
        scopeByAnchor.remove(anchor);
      }
      return scope.correlationIds;
    }

    /**
     * Returns the Calcite row type of the in-scope relation bearing the given {@code rel_anchor}.
     *
     * @param anchor the referenced {@code rel_anchor}
     * @return the row type of the binding relation
     */
    public RelDataType getAnchorRowType(final int anchor) {
      final Scope scope = requireScope(anchor);
      return scope.rowTypeByAnchor.get(anchor);
    }

    /**
     * Returns the {@link CorrelationId} for the given {@code rel_anchor}, creating it on first use
     * and attaching it to the scope that owns the anchor so the binding operator declares it.
     *
     * @param anchor the referenced {@code rel_anchor}
     * @param factory supplies a fresh correlation id when one has not yet been minted
     * @return the correlation id for this anchor
     */
    public CorrelationId correlationIdForAnchor(
        final int anchor, final java.util.function.Supplier<CorrelationId> factory) {
      final Scope scope = requireScope(anchor);
      final CorrelationId correlationId =
          correlationIdByAnchor.computeIfAbsent(anchor, k -> factory.get());
      scope.correlationIds.add(correlationId);
      return correlationId;
    }

    private Scope requireScope(final int anchor) {
      final Scope scope = scopeByAnchor.get(anchor);
      if (scope == null) {
        // The anchor is not on the active scope stack: the referenced relation is not an enclosing
        // single-input host. This includes forward references and shared subtrees reached via a
        // ReferenceRel. Signalled as unsupported, consistent with OuterReferenceConverter.
        throw new UnsupportedOperationException(
            "Outer reference rel_reference="
                + anchor
                + " has no enclosing relation with that anchor");
      }
      return scope;
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
