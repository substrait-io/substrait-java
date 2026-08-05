package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

/**
 * Substrait to Calcite conversion of the table-modification relations needs a {@link
 * Prepare.CatalogReader}, not merely a {@link RelOptSchema}. {@link
 * org.apache.calcite.rel.core.TableModify} stores the reader without checking it and only
 * dereferences it later, so an unvalidated one surfaces as a confusing failure elsewhere.
 */
class CatalogReaderValidationTest extends PlanTestBase {

  private static final List<String> TABLE = List.of("FOO");

  private final CalciteCatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog("CREATE TABLE FOO (A INT)");

  CatalogReaderValidationTest() throws SqlParseException {}

  @Test
  void namedWriteRejectsSchemaThatIsNotACatalogReader() {
    NamedWrite write =
        sb.namedWrite(
            TABLE,
            List.of("A"),
            AbstractWriteRel.WriteOp.INSERT,
            AbstractWriteRel.CreateMode.UNSPECIFIED,
            AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
            oneRowInput());

    // The write path takes its catalog reader from the RelBuilder's schema.
    assertRejects(write, schema -> new DelegatingRelOptSchema(schema, TableSchema.SELF));
  }

  @Test
  void namedUpdateRejectsTableWithoutASchema() {
    // The update path takes its catalog reader from RelOptTable.getRelOptSchema(), which Calcite
    // declares @Nullable.
    assertRejects(update(), schema -> new DelegatingRelOptSchema(schema, TableSchema.NULL));
  }

  @Test
  void namedUpdateRejectsTableSchemaThatIsNotACatalogReader() {
    assertRejects(update(), schema -> new DelegatingRelOptSchema(schema, TableSchema.SELF));
  }

  private void assertRejects(Rel rel, UnaryOperator<RelOptSchema> schemaWrapper) {
    SubstraitRelNodeConverter converter =
        new SubstraitRelNodeConverter(relBuilderWith(schemaWrapper), converterProvider);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> rel.accept(converter, SubstraitRelNodeConverter.Context.newContext()));
    assertTrue(
        e.getMessage().contains(Prepare.CatalogReader.class.getName()),
        () -> "unexpected message: " + e.getMessage());
  }

  private NamedUpdate update() {
    return sb.namedUpdate(
        TABLE,
        List.of("A"),
        List.of(
            NamedUpdate.TransformExpression.builder()
                .columnTarget(0)
                .transformation(ExpressionCreator.i32(false, 1))
                .build()),
        ExpressionCreator.bool(false, true),
        false);
  }

  private VirtualTableScan oneRowInput() {
    return VirtualTableScan.builder()
        .initialSchema(NamedStruct.of(List.of("A"), R.struct(R.I32)))
        .addRows(ExpressionCreator.nestedStruct(false, ExpressionCreator.i32(false, 1)))
        .build();
  }

  /** Builds a RelBuilder whose {@link RelOptSchema} is replaced by the given wrapper. */
  private RelBuilder relBuilderWith(UnaryOperator<RelOptSchema> schemaWrapper) {
    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .defaultSchema(catalogReader.getRootSchema().plus())
            .typeSystem(converterProvider.getTypeSystem())
            .build();
    RelBuilder relBuilder =
        Frameworks.withPrepare(
            config,
            (cluster, relOptSchema, rootSchema, statement) ->
                new SchemaOverridingRelBuilder(
                    config.getContext(), cluster, schemaWrapper.apply(relOptSchema)));
    Utils.useReflectiveMetadataProvider(relBuilder.getCluster());
    return relBuilder;
  }

  /** Exposes RelBuilder's schema-accepting constructor. */
  private static final class SchemaOverridingRelBuilder extends RelBuilder {
    private SchemaOverridingRelBuilder(
        Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }
  }

  /** What the tables handed out by {@link DelegatingRelOptSchema} report as their schema. */
  private enum TableSchema {
    /** The (non-catalog-reader) schema itself. */
    SELF,
    /** Nothing, which {@link RelOptTable#getRelOptSchema()} permits. */
    NULL
  }

  /**
   * A {@link RelOptSchema} that resolves tables through a real catalog but is deliberately not a
   * {@link Prepare.CatalogReader}.
   */
  private static final class DelegatingRelOptSchema implements RelOptSchema {
    private final RelOptSchema delegate;
    private final TableSchema tableSchema;

    DelegatingRelOptSchema(RelOptSchema delegate, TableSchema tableSchema) {
      this.delegate = delegate;
      this.tableSchema = tableSchema;
    }

    @Override
    public RelOptTable getTableForMember(List<String> names) {
      RelOptTable table = delegate.getTableForMember(names);
      if (table == null) {
        return null;
      }
      return new DelegatingRelOptTable(table, tableSchema == TableSchema.SELF ? this : null);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
      return delegate.getTypeFactory();
    }

    @Override
    public void registerRules(RelOptPlanner planner) {
      delegate.registerRules(planner);
    }
  }

  /** A {@link RelOptTable} that delegates everything but the schema it claims to belong to. */
  private static final class DelegatingRelOptTable implements RelOptTable {
    private final RelOptTable delegate;
    private final RelOptSchema relOptSchema;

    DelegatingRelOptTable(RelOptTable delegate, RelOptSchema relOptSchema) {
      this.delegate = delegate;
      this.relOptSchema = relOptSchema;
    }

    @Override
    public RelOptSchema getRelOptSchema() {
      return relOptSchema;
    }

    @Override
    public List<String> getQualifiedName() {
      return delegate.getQualifiedName();
    }

    @Override
    public double getRowCount() {
      return delegate.getRowCount();
    }

    @Override
    public RelDataType getRowType() {
      return delegate.getRowType();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
      return delegate.toRel(context);
    }

    @Override
    public List<RelCollation> getCollationList() {
      return delegate.getCollationList();
    }

    @Override
    public RelDistribution getDistribution() {
      return delegate.getDistribution();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
      return delegate.isKey(columns);
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
      return delegate.getKeys();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
      return delegate.getReferentialConstraints();
    }

    @Override
    public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
      return delegate.getExpression(clazz);
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      return delegate.extend(extendedFields);
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
      return delegate.getColumnStrategies();
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
      return delegate.unwrap(aClass);
    }
  }
}
