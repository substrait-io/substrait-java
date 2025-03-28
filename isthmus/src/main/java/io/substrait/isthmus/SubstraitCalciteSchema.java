package io.substrait.isthmus;

import com.google.common.collect.ImmutableSet;
import io.substrait.isthmus.SqlConverterBase.DefinedTable;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.NamedStruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.Named;
import org.checkerframework.checker.nullness.qual.Nullable;

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
 * <p>The change, ironically, was to support a lazy approach to looking up Calcite schemas. Good -
 * *but* the external function in Isthmus is assuming it's going to get called with a fully
 * namespaced table name. Which Calcite though sees as being subschemas.
 *
 * <p>This results in some 'complex' code below to try and map the lazy way Calcite now works and
 * maintain the existing Isthmus API
 *
 * <p>If that Ishtmus API hadn't existed, this code would be a lot simpler! Maybe a case for future
 * deprecation.
 */
public class SubstraitCalciteSchema extends AbstractSchema {

  private Map<String, Table> tables;
  private Function<List<String>, NamedStruct> tableLookup;

  private RelDataTypeFactory typeFactory;
  private TypeConverter typeConverter;

  /**
   * Maintain a track of the 'prefix' of this schema... i.e. allows recreation of the fully
   * qualified name of this subschema
   */
  private List<String> prefix = new ArrayList<>();

  protected SubstraitCalciteSchema(Map<String, Table> tables) {
    this.tables = tables;
  }

  protected SubstraitCalciteSchema(
      Function<List<String>, NamedStruct> tableLookup,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    this.tableLookup = tableLookup;
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tables;
  }

  @Override
  public Lookup<? extends Schema> subSchemas() {
    var defaultLookup = super.subSchemas();

    // Note ono the lookups, calcite prefers calling getIgnoreCase() initially

    return new Lookup<>() {

      @Override
      public @Nullable Schema get(String name) {

        // before we create the next subschema, we need to check if this
        // is actually the final value. i.e. we need to call the lookup
        // if it is the final table, we then return null here.
        // Calcite sees that, knows there are no more schemas and instead
        // calls the tables() look up to get a table name.
        var lookupNameList = new ArrayList<String>(prefix);
        lookupNameList.add(name);

        NamedStruct table = tableLookup.apply(lookupNameList);
        if (table != null) {
          return null;
        }

        var scs = new SubstraitCalciteSchema(tableLookup, typeFactory, typeConverter);
        scs.prefix = lookupNameList;
        return scs;
      }

      @Override
      public @Nullable Named<Schema> getIgnoreCase(String name) {

        // before we create the next subschema, we need to check if this
        // is actually the final value. i.e. we need to call the lookup
        // if it is the final table, we then return null here/
        // Calcite sees that there's no more schemas and instead
        // calls the tables() lazy look up to get a table name.
        var lookupNameList = new ArrayList<String>(prefix);
        lookupNameList.add(name);

        NamedStruct table = tableLookup.apply(lookupNameList);
        if (table != null) {
          return null;
        }

        var scs = new SubstraitCalciteSchema(tableLookup, typeFactory, typeConverter);
        scs.prefix = lookupNameList;
        return new Named<>(name, scs);
      }

      @Override
      public Set<String> getNames(LikePattern pattern) {
        return defaultLookup.getNames(pattern);
      }
    };
  }

  @Override
  public Lookup<Table> tables() {
    if (this.tables != null) {
      // If we do have the list of tables already specified, delegate to the super class to return
      // those
      return super.tables();
    }

    return new Lookup<Table>() {

      @Override
      public @Nullable Table get(String name) {
        List<String> p = new ArrayList<>(prefix);
        p.add(name);

        NamedStruct table = tableLookup.apply(p);
        if (table == null) {
          return null;
        }

        return new DefinedTable(
            name, typeFactory, typeConverter.toCalcite(typeFactory, table.struct(), table.names()));
      }

      @Override
      public @Nullable Named<Table> getIgnoreCase(String name) {
        /** Delegate to the noremal lookup */
        return new Named<Table>(name, get(name));
      }

      @Override
      public Set<String> getNames(LikePattern pattern) {
        return ImmutableSet.of();
      }
    };
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
    private Function<List<String>, NamedStruct> tableLookup;

    public Builder withTableLookup(Function<List<String>, NamedStruct> tableLookup) {
      this.tableLookup = tableLookup;
      return this;
    }

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
        throw new IllegalArgumentException("TypeConverter must be specified");
      }

      if (typeFactory == null) {
        throw new IllegalArgumentException("TypeFactory must be specified");
      }

      if (rel != null && tableLookup != null) {
        throw new IllegalArgumentException("Specify either 'rel' or 'tableLookup' ");
      }

      if (rel != null) {
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
      } else {
        return new SubstraitCalciteSchema(tableLookup, typeFactory, typeConverter);
      }
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
