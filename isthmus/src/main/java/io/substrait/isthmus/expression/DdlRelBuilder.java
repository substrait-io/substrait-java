package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.FeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import io.substrait.type.NamedStruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class DdlRelBuilder extends SqlBasicVisitor<Plan.Root> {
  protected final Map<Class<? extends SqlCall>, Function<SqlCall, Plan.Root>> createHandlers =
      new ConcurrentHashMap<>();

  private final SqlToRelConverter converter;
  private final BiFunction<SqlToRelConverter, SqlNode, RelRoot> bestExpRelRootGetter;
  private final SimpleExtension.ExtensionCollection extensionCollection;
  private final FeatureBoard featureBoard;

  public DdlRelBuilder(
      final SqlToRelConverter converter,
      final BiFunction<SqlToRelConverter, SqlNode, RelRoot> bestExpRelRootGetter,
      final SimpleExtension.ExtensionCollection extensionCollection,
      final FeatureBoard featureBoard) {
    super();
    this.converter = converter;
    this.bestExpRelRootGetter = bestExpRelRootGetter;
    this.extensionCollection = extensionCollection;
    this.featureBoard = featureBoard;

    createHandlers.put(
        SqlCreateTable.class, sqlCall -> handleCreateTable((SqlCreateTable) sqlCall));
    createHandlers.put(SqlCreateView.class, sqlCall -> handleCreateView((SqlCreateView) sqlCall));
  }

  private Function<SqlCall, Plan.Root> findCreateHandler(final SqlCall call) {
    Class<?> currentClass = call.getClass();
    while (SqlCall.class.isAssignableFrom(currentClass)) {
      final Function<SqlCall, Plan.Root> found = createHandlers.get(currentClass);
      if (found != null) {
        return found;
      }
      currentClass = currentClass.getSuperclass();
    }
    return null;
  }

  @Override
  public Plan.Root visit(final SqlCall sqlCall) {
    Function<SqlCall, Plan.Root> createHandler = findCreateHandler(sqlCall);
    if (createHandler == null) {
      return null;
    }

    return createHandler.apply(sqlCall);
  }

  private NamedStruct getSchema(final RelRoot queryRelRoot) {
    final RelDataType rowType = queryRelRoot.rel.getRowType();

    final TypeConverter typeConverter = TypeConverter.DEFAULT;
    return typeConverter.toNamedStruct(rowType);
  }

  protected Plan.Root handleCreateTable(final SqlCreateTable sqlCreateTable) {
    if (sqlCreateTable.query == null) {
      throw new IllegalArgumentException("Only create table as select statements are supported");
    }

    final RelRoot queryRelRoot = bestExpRelRootGetter.apply(converter, sqlCreateTable.query);

    NamedStruct schema = getSchema(queryRelRoot);

    Plan.Root rel = SubstraitRelVisitor.convert(queryRelRoot, extensionCollection, featureBoard);
    NamedWrite namedWrite =
        NamedWrite.builder()
            .input(rel.getInput())
            .tableSchema(schema)
            .operation(AbstractWriteRel.WriteOp.CTAS)
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .names(sqlCreateTable.name.names)
            .build();

    return Plan.Root.builder().input(namedWrite).build();
  }

  protected Plan.Root handleCreateView(final SqlCreateView sqlCreateView) {

    final RelRoot queryRelRoot = bestExpRelRootGetter.apply(converter, sqlCreateView.query);
    Plan.Root rel = SubstraitRelVisitor.convert(queryRelRoot, extensionCollection, featureBoard);
    final Expression.StructLiteral defaults = ExpressionCreator.struct(false);

    final NamedDdl namedDdl =
        NamedDdl.builder()
            .viewDefinition(rel.getInput())
            .tableSchema(getSchema(queryRelRoot))
            .tableDefaults(defaults)
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .names(sqlCreateView.name.names)
            .build();

    return Plan.Root.builder().input(namedDdl).build();
  }
}
