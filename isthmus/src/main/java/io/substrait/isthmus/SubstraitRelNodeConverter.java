package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter extends AbstractRelVisitor<RelNode, RuntimeException> {

  private final RelOptCluster relOptCluster;
  private final CalciteCatalogReader catalogReader;

  private final SimpleExtension.ExtensionCollection extensions;

  private final ScalarFunctionConverter scalarFunctionConverter;

  private final AggregateFunctionConverter aggregateFunctionConverter;
  private final ExpressionRexConverter expressionRexConverter;

  private final RelBuilder relBuilder;

  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelOptCluster relOptCluster,
      CalciteCatalogReader catalogReader,
      SqlParser.Config parserConfig) {
    this.relOptCluster = relOptCluster;
    this.catalogReader = catalogReader;
    this.extensions = extensions;

    this.relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    this.scalarFunctionConverter =
        new ScalarFunctionConverter(
            this.extensions.scalarFunctions(), relOptCluster.getTypeFactory());

    this.aggregateFunctionConverter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), relOptCluster.getTypeFactory());

    this.expressionRexConverter =
        new ExpressionRexConverter(
            relOptCluster.getTypeFactory(), scalarFunctionConverter, aggregateFunctionConverter);
  }

  public static RelNode convert(
      Rel relRoot,
      RelOptCluster relOptCluster,
      CalciteCatalogReader calciteCatalogReader,
      SqlParser.Config parserConfig) {
    return relRoot.accept(
        new SubstraitRelNodeConverter(
            EXTENSION_COLLECTION, relOptCluster, calciteCatalogReader, parserConfig));
  }

  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    RelNode input = filter.getInput().accept(this);
    RexNode filterCondition = filter.getCondition().accept(expressionRexConverter);
    return relBuilder.push(input).filter(filterCondition).build();
  }

  @Override
  public RelNode visit(NamedScan namedScan) throws RuntimeException {
    return relBuilder.scan(namedScan.getNames()).build();
  }

  @Override
  public RelNode visit(Project project) throws RuntimeException {
    RelNode child = project.getInput().accept(this);
    List<RexNode> rexList =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter)).toList();

    return relBuilder.push(child).project(rexList).build();
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
    return relBuilder.push(left).push(right).join(joinType, condition).build();
  }

  @Override
  public RelNode visit(Aggregate aggregate) throws RuntimeException {
    RelNode child = aggregate.getInput().accept(this);
    var groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter))
                        .toList())
            .toList();
    List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs, groupExprLists);

    List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream().map(this::fromMeasure).toList();
    return relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
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
            .toList();
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
      // TODO: rewrite compound expression into project Rel
      checkRexInputRefOnly(arg, "argument", measure.getFunction().declaration().name());
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    boolean distinct =
        switch (measure.getFunction().invocation()) {
          case AGGREGATION_INVOCATION_DISTINCT:
            yield true;
          default:
            yield false;
        };

    SqlAggFunction aggFunction;
    RelDataType returnType =
        TypeConverter.convert(relOptCluster.getTypeFactory(), measure.getFunction().getType());

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
      // TODO: rewrite compound expression into project Rel
      // Calcite's AggregateCall only allow agg filter to be a direct filter from input
      checkRexInputRefOnly(filter, "filter", measure.getFunction().declaration().name());
      filterArg = ((RexInputRef) filter).getIndex();
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        argIndex,
        filterArg,
        null,
        RelCollations.EMPTY,
        returnType,
        null);
  }

  @Override
  public RelNode visitFallback(Rel rel) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
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
