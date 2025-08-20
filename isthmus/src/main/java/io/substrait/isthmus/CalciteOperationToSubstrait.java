package io.substrait.isthmus;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.operation.CalciteOperationBasicVisitor;
import io.substrait.isthmus.operation.CreateTableAs;
import io.substrait.isthmus.operation.CreateView;
import io.substrait.isthmus.operation.RelationalOperation;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import io.substrait.type.NamedStruct;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

public class CalciteOperationToSubstrait extends CalciteOperationBasicVisitor<Plan.Root> {
  private final SimpleExtension.ExtensionCollection extensionCollection;
  private final FeatureBoard featureBoard;

  public CalciteOperationToSubstrait(
      SimpleExtension.ExtensionCollection extensionCollection, FeatureBoard featureBoard) {
    this.extensionCollection = extensionCollection;
    this.featureBoard = featureBoard;
  }

  private NamedStruct getSchema(final RelRoot queryRelRoot) {
    final RelDataType rowType = queryRelRoot.rel.getRowType();

    final TypeConverter typeConverter = TypeConverter.DEFAULT;
    return typeConverter.toNamedStruct(rowType);
  }

  @Override
  public Plan.Root visit(RelationalOperation relationalOperation) {
    return SubstraitRelVisitor.convert(
        relationalOperation.getRelRoot(), extensionCollection, featureBoard);
  }

  @Override
  public Plan.Root visit(CreateTableAs createTableAs) {
    RelRoot input = createTableAs.getInput();
    Plan.Root rel = SubstraitRelVisitor.convert(input, extensionCollection, featureBoard);
    NamedStruct schema = getSchema(input);

    NamedWrite namedWrite =
        NamedWrite.builder()
            .input(rel.getInput())
            .tableSchema(schema)
            .operation(AbstractWriteRel.WriteOp.CTAS)
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .names(createTableAs.getNames())
            .build();

    return Plan.Root.builder().input(namedWrite).build();
  }

  @Override
  public Plan.Root visit(CreateView createView) {
    RelRoot input = createView.getInput();
    Plan.Root rel = SubstraitRelVisitor.convert(input, extensionCollection, featureBoard);
    final Expression.StructLiteral defaults = ExpressionCreator.struct(false);

    final NamedDdl namedDdl =
        NamedDdl.builder()
            .viewDefinition(rel.getInput())
            .tableSchema(getSchema(input))
            .tableDefaults(defaults)
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .names(createView.getNames())
            .build();

    return Plan.Root.builder().input(namedDdl).build();
  }
}
