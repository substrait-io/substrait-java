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
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/**
 * Substrait to Calcite conversion of the table-modification relations needs a catalog to resolve
 * the target table, and that catalog must be a {@link Prepare.CatalogReader}, not merely a {@link
 * RelOptSchema}. {@link org.apache.calcite.rel.core.TableModify} stores the reader without checking
 * it and only dereferences it later, so an unvalidated one surfaces as a confusing failure
 * elsewhere.
 */
class CatalogReaderValidationTest extends PlanTestBase {

  private static final List<String> TABLE = List.of("FOO");

  private final CalciteCatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog("CREATE TABLE FOO (A INT)");

  CatalogReaderValidationTest() throws SqlParseException {}

  @Test
  void namedWriteRequiresACatalogBackedRelBuilder() {
    // A RelBuilder can be built without a catalog at all; RelBuilder.getRelOptSchema() is nullable.
    SubstraitRelNodeConverter converter =
        new SubstraitRelNodeConverter(relBuilderWith(schema -> null), converterProvider);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> write(TABLE).accept(converter, SubstraitRelNodeConverter.Context.newContext()));
    assertTrue(
        e.getMessage().contains("has no RelOptSchema"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  void namedWriteRejectsUnknownTable() {
    SubstraitRelNodeConverter converter =
        new SubstraitRelNodeConverter(relBuilderWith(UnaryOperator.identity()), converterProvider);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                write(List.of("MISSING"))
                    .accept(converter, SubstraitRelNodeConverter.Context.newContext()));
    assertTrue(
        e.getMessage().contains("Table not found in Calcite catalog"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  void namedWriteRejectsSchemaThatIsNotACatalogReader() {
    // The write path takes its catalog reader from the RelBuilder's schema.
    assertRejects(write(TABLE), schema -> new DelegatingRelOptSchema(schema, TableSchema.SELF));
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

  private NamedWrite write(List<String> names) {
    return sb.namedWrite(
        names,
        List.of("A"),
        AbstractWriteRel.WriteOp.INSERT,
        AbstractWriteRel.CreateMode.UNSPECIFIED,
        AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
        oneRowInput());
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
      // Re-create the resolved table so that the schema it reports is this one (or none).
      return RelOptTableImpl.create(
          tableSchema == TableSchema.SELF ? this : null,
          table.getRowType(),
          table.getQualifiedName(),
          Expressions.constant(null));
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
}
