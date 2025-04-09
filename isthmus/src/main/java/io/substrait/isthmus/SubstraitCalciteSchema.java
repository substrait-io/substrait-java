package io.substrait.isthmus;

import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.NamedStruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;

/**
 * A subclass of the Calcite Schema for creation from a Substrait relation
 *
 * <p>Implementation note:
 *
 * <p>The external Isthmus API can take a function that will return the table schema when needed,
 * rather than it being available up front.
 *
 * <p>This was implemented by a special subclass of the Calcite simple schema. Since this was
 * changed in Calcite 1.39.0; it failed to work; the protected methods it extended from changed.
 *
 * <p>The new feature of the Calcite schema is the 'lazy' or delayed lookup of tables. This feature
 * has not be exploited here
 */
public class SubstraitCalciteSchema extends AbstractSchema {

  private Map<String, Table> tables;

  protected SubstraitCalciteSchema(Map<String, Table> tables) {
    this.tables = tables;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tables;
  }

  @Override
  public Lookup<Table> tables() {
    return super.tables();
  }

  /**
   * Turn this into a root Calciteschema Choice of settings is based on current isthmus behaviour
   */
  public CalciteSchema getRootSchema() {
    return CalciteSchema.createRootSchema(false, false, "", this);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to assist with creating the CalciteSchema
   *
   * <p>Can be created from a Rel or a Lookup function
   */
  public static class Builder {

    private Rel rel;
    private RelDataTypeFactory typeFactory;
    private TypeConverter typeConverter;

    public Builder withTypeFactory(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
      return this;
    }

    public Builder withTypeConverter(TypeConverter typeConverter) {
      this.typeConverter = typeConverter;
      return this;
    }

    public Builder withSubstraitRel(Rel rel) {
      this.rel = rel;
      return this;
    }

    public SubstraitCalciteSchema build() {
      if (typeConverter == null) {
        throw new IllegalArgumentException("'TypeConverter' must be specified");
      }

      if (typeFactory == null) {
        throw new IllegalArgumentException("'TypeFactory' must be specified");
      }

      if (rel == null) {
        throw new IllegalArgumentException("'rel' must be specified");
      }

      // If there are any named structs within the relation, gather these and convert
      // them to a map of tables
      // index by name; note that the name of the table is 'un-namespaced' here.
      // This was the existing logic so it has not been altered.
      Map<List<String>, NamedStruct> tableMap = NamedStructGatherer.gatherTables(rel);

      Map<String, Table> tables =
          tableMap.entrySet().stream()
              .map(
                  entry -> {
                    var id = entry.getKey();
                    var name = id.get(id.size() - 1);
                    var table = entry.getValue();
                    var value =
                        new SqlConverterBase.DefinedTable(
                            name,
                            typeFactory,
                            typeConverter.toCalcite(typeFactory, table.struct(), table.names()));
                    return Map.entry(name, value);
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return new SubstraitCalciteSchema(tables);
    }
  }

  private static final class NamedStructGatherer extends RelCopyOnWriteVisitor<RuntimeException> {
    Map<List<String>, NamedStruct> tableMap;

    private NamedStructGatherer() {
      super();
      this.tableMap = new HashMap<>();
    }

    public static Map<List<String>, NamedStruct> gatherTables(Rel rel) {
      var visitor = new NamedStructGatherer();
      rel.accept(visitor);
      return visitor.tableMap;
    }

    @Override
    public Optional<Rel> visit(NamedScan namedScan) {
      Optional<Rel> result = super.visit(namedScan);

      List<String> tableName = namedScan.getNames();
      tableMap.put(tableName, namedScan.getInitialSchema());

      return result;
    }
  }
}
