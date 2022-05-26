package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

public class SubstraitRelNodeConverter extends AbstractRelVisitor<RelNode, RuntimeException> {

  private final RelOptCluster relOptCluster;
  private final CalciteCatalogReader catalogReader;

  private final SimpleExtension.ExtensionCollection extensions;

  private final ScalarFunctionConverter scalarFunctionConverter;

  private final RelBuilder relBuilder;

  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelOptCluster relOptCluster,
      CalciteCatalogReader catalogReader) {
    this.relOptCluster = relOptCluster;
    this.catalogReader = catalogReader;
    this.extensions = extensions;

    this.relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    this.scalarFunctionConverter =
        new ScalarFunctionConverter(
            this.extensions.scalarFunctions(), relOptCluster.getTypeFactory());
  }

  public static RelNode convert(
      Rel relRoot, RelOptCluster relOptCluster, CalciteCatalogReader calciteCatalogReader) {
    return relRoot.accept(
        new SubstraitRelNodeConverter(EXTENSION_COLLECTION, relOptCluster, calciteCatalogReader));
  }

  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    RelNode input = filter.getInput().accept(this);
    RexNode filterCondition =
        filter
            .getCondition()
            .accept(
                new ExpressionRexConverter(
                    relOptCluster.getTypeFactory(), scalarFunctionConverter));
    return relBuilder.push(input).filter(filterCondition).build();

    // return LogicalFilter.create(input, filterCondition);
  }

  @Override
  public RelNode visit(NamedScan namedScan) throws RuntimeException {
    return relBuilder.scan(namedScan.getNames()).build();
  }

  @Override
  public RelNode visit(Project project) throws RuntimeException {
    RelNode child = project.getInput().accept(this);
    List<RexNode> rexList =
        project.getExpressions().stream()
            .map(
                expr ->
                    expr.accept(
                        new ExpressionRexConverter(
                            relOptCluster.getTypeFactory(), scalarFunctionConverter)))
            .toList();

    return relBuilder.push(child).project(rexList).build();
  }

  @Override
  public RelNode visitFallback(Rel rel) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel of type %s not handled by visitor type %s.",
            rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }
}
