package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter extends AbstractRelVisitor<RelNode, RuntimeException> {

  protected final RelDataTypeFactory typeFactory;

  protected final ScalarFunctionConverter scalarFunctionConverter;
  protected final AggregateFunctionConverter aggregateFunctionConverter;
  protected final ExpressionRexConverter expressionRexConverter;

  protected final RelBuilder relBuilder;
  protected final RexBuilder rexBuilder;
  private final TypeConverter typeConverter;

  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelDataTypeFactory typeFactory,
      RelBuilder relBuilder) {
    this(
        typeFactory,
        relBuilder,
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory),
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
  }

  public SubstraitRelNodeConverter(
      RelDataTypeFactory typeFactory,
      RelBuilder relBuilder,
      ScalarFunctionConverter scalarFunctionConverter,
      AggregateFunctionConverter aggregateFunctionConverter,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter) {
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
      RelDataTypeFactory typeFactory,
      RelBuilder relBuilder,
      ScalarFunctionConverter scalarFunctionConverter,
      AggregateFunctionConverter aggregateFunctionConverter,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter,
      ExpressionRexConverter expressionRexConverter) {
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
      Rel relRoot,
      RelOptCluster relOptCluster,
      Prepare.CatalogReader catalogReader,
      SqlParser.Config parserConfig) {
    var relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    return relRoot.accept(
        new SubstraitRelNodeConverter(
            EXTENSION_COLLECTION, relOptCluster.getTypeFactory(), relBuilder));
  }

  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    RelNode input = filter.getInput().accept(this);
    RexNode filterCondition = filter.getCondition().accept(expressionRexConverter);
    RelNode node = relBuilder.push(input).filter(filterCondition).build();
    return applyRemap(node, filter.getRemap());
  }

  @Override
  public RelNode visit(NamedScan namedScan) throws RuntimeException {
    RelNode node = relBuilder.scan(namedScan.getNames()).build();
    return applyRemap(node, namedScan.getRemap());
  }

  @Override
  public RelNode visit(LocalFiles localFiles) throws RuntimeException {
    return visitFallback(localFiles);
  }

  @Override
  public RelNode visit(Project project) throws RuntimeException {
    RelNode child = project.getInput().accept(this);

    Stream<RexNode> directOutputs =
        IntStream.range(0, child.getRowType().getFieldCount())
            .mapToObj(fieldIndex -> rexBuilder.makeInputRef(child, fieldIndex));

    Stream<RexNode> exprs =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter));

    List<RexNode> rexExprs =
        Stream.concat(directOutputs, exprs).collect(java.util.stream.Collectors.toList());

    RelNode node = relBuilder.push(child).project(rexExprs).build();
    return applyRemap(node, project.getRemap());
  }

  @Override
  public RelNode visit(Cross cross) throws RuntimeException {
    RelNode left = cross.getLeft().accept(this);
    RelNode right = cross.getRight().accept(this);
    // Calcite represents CROSS JOIN as the equivalent INNER JOIN with true condition
    RelNode node =
        relBuilder.push(left).push(right).join(JoinRelType.INNER, relBuilder.literal(true)).build();
    return applyRemap(node, cross.getRemap());
  }

  @Override
  public RelNode visit(Join join) throws RuntimeException {
    var left = join.getLeft().accept(this);
    var right = join.getRight().accept(this);
    var condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter))
            .orElse(relBuilder.literal(true));
    var joinType =
        switch (join.getJoinType()) {
          case INNER -> JoinRelType.INNER;
          case LEFT -> JoinRelType.LEFT;
          case RIGHT -> JoinRelType.RIGHT;
          case OUTER -> JoinRelType.FULL;
          case SEMI -> JoinRelType.SEMI;
          case ANTI -> JoinRelType.ANTI;
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown join type is not supported");
        };
    RelNode node = relBuilder.push(left).push(right).join(joinType, condition).build();
    return applyRemap(node, join.getRemap());
  }

  @Override
  public RelNode visit(Set set) throws RuntimeException {
    int numInputs = set.getInputs().size();
    set.getInputs()
        .forEach(
            input -> {
              relBuilder.push(input.accept(this));
            });
    var builder =
        switch (set.getSetOp()) {
          case MINUS_PRIMARY -> relBuilder.minus(false, numInputs);
          case MINUS_MULTISET -> relBuilder.minus(true, numInputs);
          case INTERSECTION_PRIMARY -> relBuilder.intersect(false, numInputs);
          case INTERSECTION_MULTISET -> relBuilder.intersect(true, numInputs);
          case UNION_DISTINCT -> relBuilder.union(false, numInputs);
          case UNION_ALL -> relBuilder.union(true, numInputs);
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown set operation is not supported");
        };
    RelNode node = builder.build();
    return applyRemap(node, set.getRemap());
  }

  @Override
  public RelNode visit(Aggregate aggregate) throws RuntimeException {
    if (!PreCalciteAggregateValidator.isValidCalciteAggregate(aggregate)) {
      aggregate =
          PreCalciteAggregateValidator.PreCalciteAggregateTransformer
              .transformToValidCalciteAggregate(aggregate);
    }

    RelNode child = aggregate.getInput().accept(this);
    var groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter))
                        .collect(java.util.stream.Collectors.toList()))
            .collect(java.util.stream.Collectors.toList());
    List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs, groupExprLists);

    List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream()
            .map(this::fromMeasure)
            .collect(java.util.stream.Collectors.toList());
    RelNode node = relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
    return applyRemap(node, aggregate.getRemap());
  }

  private AggregateCall fromMeasure(Aggregate.Measure measure) {
    var eArgs = measure.getFunction().arguments();
    var arguments =
        IntStream.range(0, measure.getFunction().arguments().size())
            .mapToObj(
                i ->
                    eArgs
                        .get(i)
                        .accept(measure.getFunction().declaration(), i, expressionRexConverter))
            .collect(java.util.stream.Collectors.toList());
    var operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(), measure.getFunction().outputType());
    if (!operator.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    List<Integer> argIndex = new ArrayList<>();
    for (RexNode arg : arguments) {
      // arguments are guaranteed to be RexInputRef because of the prior call to
      // AggregateValidator.AggregateTransformer.transformToValidCalciteAggregate
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
      RexNode filter = measure.getPreMeasureFilter().get().accept(expressionRexConverter);
      filterArg = ((RexInputRef) filter).getIndex();
    }

    RelCollation relCollation = RelCollations.EMPTY;
    if (!measure.getFunction().sort().isEmpty()) {
      relCollation =
          RelCollations.of(
              measure.getFunction().sort().stream()
                  .map(sortField -> toRelFieldCollation(sortField))
                  .collect(Collectors.toList()));
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        argIndex,
        filterArg,
        null,
        relCollation,
        returnType,
        null);
  }

  @Override
  public RelNode visit(Sort sort) throws RuntimeException {
    RelNode child = sort.getInput().accept(this);
    List<RelFieldCollation> relFieldCollations =
        sort.getSortFields().stream()
            .map(sortField -> toRelFieldCollation(sortField))
            .collect(java.util.stream.Collectors.toList());
    if (relFieldCollations.isEmpty()) {
      return relBuilder.push(child).sort(Collections.EMPTY_LIST).build();
    }
    RelNode node = relBuilder.push(child).sort(RelCollations.of(relFieldCollations)).build();
    return applyRemap(node, sort.getRemap());
  }

  @Override
  public RelNode visit(Fetch fetch) throws RuntimeException {
    RelNode child = fetch.getInput().accept(this);
    var optCount = fetch.getCount();
    long count = optCount.orElse(-1L);
    var offset = fetch.getOffset();
    if (offset > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("offset is overflowed as an integer: %d", offset));
    }
    if (count > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("count is overflowed as an integer: %d", count));
    }
    RelNode node = relBuilder.push(child).limit((int) offset, (int) count).build();
    return applyRemap(node, fetch.getRemap());
  }

  private RelFieldCollation toRelFieldCollation(Expression.SortField sortField) {
    var expression = sortField.expr();
    var rex = expression.accept(expressionRexConverter);
    var sortDirection = sortField.direction();
    RexSlot rexSlot = (RexSlot) rex;
    int fieldIndex = rexSlot.getIndex();
    var fieldDirection = RelFieldCollation.Direction.ASCENDING;
    var nullDirection = RelFieldCollation.NullDirection.UNSPECIFIED;
    switch (sortDirection) {
      case ASC_NULLS_FIRST -> nullDirection = RelFieldCollation.NullDirection.FIRST;
      case ASC_NULLS_LAST -> nullDirection = RelFieldCollation.NullDirection.LAST;
      case DESC_NULLS_FIRST -> {
        nullDirection = RelFieldCollation.NullDirection.FIRST;
        fieldDirection = RelFieldCollation.Direction.DESCENDING;
      }
      case DESC_NULLS_LAST -> {
        nullDirection = RelFieldCollation.NullDirection.LAST;
        fieldDirection = RelFieldCollation.Direction.DESCENDING;
      }
      case CLUSTERED -> fieldDirection = RelFieldCollation.Direction.CLUSTERED;

      default -> throw new RuntimeException(
          String.format("Unexpected Expression.SortDirection enum: %s !", sortDirection));
    }
    return new RelFieldCollation(fieldIndex, fieldDirection, nullDirection);
  }

  @Override
  public RelNode visitFallback(Rel rel) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  protected RelNode applyRemap(RelNode relNode, Optional<Rel.Remap> remap) {
    if (remap.isPresent()) {
      return applyRemap(relNode, remap.get());
    }
    return relNode;
  }

  private RelNode applyRemap(RelNode relNode, Rel.Remap remap) {
    var rowType = relNode.getRowType();
    var fieldNames = rowType.getFieldNames();
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

  private void checkRexInputRefOnly(RexNode rexNode, String context, String aggName) {
    if (!(rexNode instanceof RexInputRef)) {
      throw new UnsupportedOperationException(
          String.format(
              "Compound expression %s in %s of agg function %s is not implemented yet.",
              rexNode, context, aggName));
    }
  }
}
