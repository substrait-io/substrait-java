package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.operation.CalciteOperation;
import io.substrait.isthmus.operation.CreateTableAs;
import io.substrait.isthmus.operation.CreateView;
import io.substrait.isthmus.operation.RelationalOperation;
import io.substrait.isthmus.operation.SqlKindFromRel;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Rel;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

public class SubstraitToCalciteOperation
    extends AbstractRelVisitor<
        CalciteOperation, SubstraitRelNodeConverter.Context, RuntimeException> {

  private final SubstraitRelNodeConverter substraitRelNodeConverter;

  public SubstraitToCalciteOperation(
      SimpleExtension.ExtensionCollection extensionCollection,
      RelDataTypeFactory relDataTypeFactory,
      Prepare.CatalogReader catalogReader,
      SqlParser.Config parserConfig) {

    RelBuilder relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());
    this.substraitRelNodeConverter =
        new SubstraitRelNodeConverter(extensionCollection, relDataTypeFactory, relBuilder);
  }

  @Override
  public CalciteOperation visit(NamedDdl namedDdl, SubstraitRelNodeConverter.Context ctx) {
    if (namedDdl.getViewDefinition().isEmpty()) {
      throw new IllegalArgumentException("no view definition found");
    }
    Rel viewDefinition = namedDdl.getViewDefinition().get();
    RelNode relNode = viewDefinition.accept(substraitRelNodeConverter, ctx);
    RelRoot relRoot = RelRoot.of(relNode, SqlKind.CREATE_VIEW);

    return new CreateView(namedDdl.getNames(), relRoot);
  }

  @Override
  public CalciteOperation visit(NamedWrite namedWrite, SubstraitRelNodeConverter.Context ctx) {
    Rel input = namedWrite.getInput();
    RelNode relNode = input.accept(substraitRelNodeConverter, ctx);
    RelRoot relRoot = RelRoot.of(relNode, SqlKind.CREATE_TABLE);
    return new CreateTableAs(namedWrite.getNames(), relRoot);
  }

  @Override
  public CalciteOperation visitFallback(Rel rel, SubstraitRelNodeConverter.Context context) {
    SqlKindFromRel sqlKindFromRel = new SqlKindFromRel();
    SqlKind kind = rel.accept(sqlKindFromRel, EmptyVisitationContext.INSTANCE);
    RelNode relNode = rel.accept(substraitRelNodeConverter, context);
    RelRoot relRoot = RelRoot.of(relNode, kind);
    return new RelationalOperation(relRoot);
  }
}
