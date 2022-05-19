package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import com.google.common.collect.ImmutableList;
import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.*;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class SubstraitRelNodeConverter implements RelVisitor<RelNode, RuntimeException> {

  private final RelOptCluster relOptCluster;
  private final CalciteCatalogReader catalogReader;

  private final SimpleExtension.ExtensionCollection extensions;

  private final ScalarFunctionConverter scalarFunctionConverter;

  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelOptCluster relOptCluster,
      CalciteCatalogReader catalogReader) {
    this.relOptCluster = relOptCluster;
    this.catalogReader = catalogReader;
    this.extensions = extensions;
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
  public RelNode visit(Aggregate aggregate) throws RuntimeException {
    throw new UnsupportedOperationException("Aggregate is not supported in SubstraitToSql!");
  }

  @Override
  public RelNode visit(EmptyScan emptyScan) throws RuntimeException {
    throw new UnsupportedOperationException("EmptyScan is not supported in SubstraitToSql!");
  }

  @Override
  public RelNode visit(Fetch fetch) throws RuntimeException {
    throw new UnsupportedOperationException("Fetch is not supported in SubstraitToSql!");
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
    return LogicalFilter.create(input, filterCondition);
  }

  @Override
  public RelNode visit(Join join) throws RuntimeException {
    throw new UnsupportedOperationException("Join is not supported in SubstraitToSql!");
  }

  @Override
  public RelNode visit(NamedScan namedScan) throws RuntimeException {
    RelDataType rowType =
        TypeConverter.convert(
            relOptCluster.getTypeFactory(),
            namedScan.getRecordType(),
            namedScan.getInitialSchema().names());
    CalciteSchema.TableEntry entry =
        catalogReader.getRootSchema().getTable(namedScan.getNames().get(0), false);
    if (entry == null) {
      throw new RuntimeException("Table " + namedScan.getNames() + " not found in catalog");
    }
    RelOptTable table = RelOptTableImpl.create(catalogReader, rowType, entry, null /*rowcount*/);
    return LogicalTableScan.create(relOptCluster, table, ImmutableList.of() /*hints*/);
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

    return LogicalProject.create(
        child,
        ImmutableList.of(),
        rexList,
        TypeConverter.convert(relOptCluster.getTypeFactory(), project.getRecordType()));
  }

  @Override
  public RelNode visit(Sort sort) throws RuntimeException {
    throw new UnsupportedOperationException("Sort is not supported in SubstraitToSql!");
  }

  @Override
  public RelNode visit(VirtualTableScan virtualTableScan) throws RuntimeException {
    throw new UnsupportedOperationException("VirtualTableScan is not supported in SubstraitToSql!");
  }
}
