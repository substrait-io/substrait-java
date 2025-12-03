package io.substrait.isthmus;

import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.NamedStruct;
import io.substrait.util.EmptyVisitationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.jspecify.annotations.NonNull;

/** For use in generating {@link CalciteSchema}s from Substrait {@link Rel}s */
public class SchemaCollector {

  private static final boolean CASE_SENSITIVE = false;

  private final RelDataTypeFactory typeFactory;
  private final TypeConverter typeConverter;

  public SchemaCollector(final RelDataTypeFactory typeFactory, final TypeConverter typeConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
  }

  /**
   * Returns a {@link CalciteSchema} containing all tables and schemas defined in {@link NamedScan}s
   * and {@link NamedWrite}s within the provided relation operation tree.
   *
   * @param rel the relation operation tree to gather the tables and schemas from, must not be null
   * @return the {@link CalciteSchema} containing the discovered tables and schemas
   */
  public CalciteSchema toSchema(@NonNull final Rel rel) {
    // Create the root schema under which all tables and schemas will be nested.
    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);

    for (final Map.Entry<List<String>, NamedStruct> entry :
        TableGatherer.gatherTables(rel).entrySet()) {
      final List<String> names = entry.getKey();
      final NamedStruct namedStruct = entry.getValue();

      // The last name in names is the table name. All others are schema names.
      final String tableName = names.get(names.size() - 1);

      final CalciteSchema schema =
          Utils.createCalciteSchemaFromNames(rootSchema, names.subList(0, names.size() - 1));

      // Create the table if it is not present
      final CalciteSchema.TableEntry table = schema.getTable(tableName, CASE_SENSITIVE);
      if (table == null) {
        final RelDataType rowType =
            typeConverter.toCalcite(typeFactory, namedStruct.struct(), namedStruct.names());
        schema.add(tableName, new SubstraitTable(tableName, rowType));
      }
    }

    return rootSchema;
  }

  static class TableGatherer extends RelCopyOnWriteVisitor<RuntimeException> {
    Map<List<String>, NamedStruct> tableMap;

    private TableGatherer() {
      super();
      this.tableMap = new HashMap<>();
    }

    /**
     * Gathers all tables defined in {@link NamedScan}s and {@link NamedWrite}s under the given
     * {@link Rel}
     *
     * @param rootRel under which to search for {@link NamedScan}s
     * @return a map of qualified table names to their associated Substrait schemas
     */
    public static Map<List<String>, NamedStruct> gatherTables(final Rel rootRel) {
      final TableGatherer visitor = new TableGatherer();
      rootRel.accept(visitor, EmptyVisitationContext.INSTANCE);
      return visitor.tableMap;
    }

    @Override
    public Optional<Rel> visit(final NamedScan namedScan, final EmptyVisitationContext context) {
      super.visit(namedScan, context);

      final List<String> tableName = namedScan.getNames();
      if (tableMap.containsKey(tableName)) {
        final NamedStruct existingSchema = tableMap.get(tableName);
        if (!existingSchema.equals(namedScan.getInitialSchema())) {
          throw new IllegalArgumentException(
              String.format(
                  "NamedScan for %s is present multiple times with different schemas", tableName));
        }
      }
      tableMap.put(tableName, namedScan.getInitialSchema());

      return Optional.empty();
    }

    @Override
    public Optional<Rel> visit(final NamedWrite namedWrite, final EmptyVisitationContext context) {
      super.visit(namedWrite, context);
      final List<String> tableName = namedWrite.getNames();

      if (tableMap.containsKey(tableName)) {
        final NamedStruct existingSchema = tableMap.get(tableName);
        if (!existingSchema.equals(namedWrite.getTableSchema())) {
          throw new IllegalArgumentException(
              String.format(
                  "NamedWrite for %s is present multiple times with different schemas", tableName));
        }
      }
      tableMap.put(tableName, namedWrite.getTableSchema());

      return Optional.empty();
    }

    @Override
    public Optional<Rel> visit(
        final NamedUpdate namedUpdate, final EmptyVisitationContext context) {
      super.visit(namedUpdate, context);
      final List<String> tableName = namedUpdate.getNames();

      if (tableMap.containsKey(tableName)) {
        final NamedStruct existingSchema = tableMap.get(tableName);
        if (!existingSchema.equals(namedUpdate.getTableSchema())) {
          throw new IllegalArgumentException(
              String.format(
                  "NamedUpdate for %s is present multiple times with different schemas",
                  tableName));
        }
      }
      tableMap.put(tableName, namedUpdate.getTableSchema());

      return Optional.empty();
    }
  }
}
