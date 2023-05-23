package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * Converts between Substrait {@link Rel}s and Calcite {@link RelNode}s.
 *
 * <p>Can be extended to customize the {@link RelBuilder} and {@link SubstraitRelNodeConverter} used
 * in the conversion.
 */
public class SubstraitToCalcite {

  private final SimpleExtension.ExtensionCollection extensions;
  private final RelDataTypeFactory typeFactory;

  public SubstraitToCalcite(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    this.extensions = extensions;
    this.typeFactory = typeFactory;
  }

  /**
   * Extracts a {@link CalciteSchema} from a {@link Rel}
   *
   * <p>Override this method to customize schema extraction.
   */
  protected CalciteSchema toSchema(Rel rel) {
    Map<List<String>, NamedStruct> tableMap = NamedStructGatherer.gatherTables(rel);
    Function<List<String>, Table> lookup =
        id -> {
          NamedStruct table = tableMap.get(id);
          if (table == null) {
            return null;
          }
          return new SqlConverterBase.DefinedTable(
              id.get(id.size() - 1),
              typeFactory,
              TypeConverter.convert(typeFactory, table.struct(), table.names()));
        };
    return LookupCalciteSchema.createRootSchema(lookup);
  }

  /**
   * Creates a {@link RelBuilder} from the extracted {@link CalciteSchema}
   *
   * <p>Override this method to customize the {@link RelBuilder}.
   */
  protected RelBuilder createRelBuilder(CalciteSchema schema) {
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema.plus()).build());
  }

  /**
   * Creates a {@link SubstraitRelNodeConverter} from the {@link RelBuilder}
   *
   * <p>Override this method to customize the {@link SubstraitRelNodeConverter}.
   */
  protected SubstraitRelNodeConverter createSubstraitRelNodeConverter(RelBuilder relBuilder) {
    return new SubstraitRelNodeConverter(extensions, typeFactory, relBuilder);
  }

  /**
   * Converts a Substrait {@link Rel} to a Calcite {@link RelNode}
   *
   * <p>Generates a {@link CalciteSchema} based on the contents of the {@link Rel}, which will be
   * used to construct a {@link RelBuilder} with the required schema information to build {@link
   * RelNode}s, and a then a {@link SubstraitRelNodeConverter} to perform the actual conversion.
   *
   * @param rel {@link Rel} to convert
   * @return {@link RelNode}
   */
  public RelNode convert(Rel rel) {
    CalciteSchema rootSchema = toSchema(rel);
    RelBuilder relBuilder = createRelBuilder(rootSchema);
    SubstraitRelNodeConverter converter = createSubstraitRelNodeConverter(relBuilder);
    return rel.accept(converter);
  }

  private static class NamedStructGatherer extends AbstractRelVisitor<Void, RuntimeException> {
    Map<List<String>, NamedStruct> tableMap;

    private NamedStructGatherer() {
      this.tableMap = new HashMap<>();
    }

    public static Map<List<String>, NamedStruct> gatherTables(Rel rel) {
      var visitor = new NamedStructGatherer();
      rel.accept(visitor);
      return visitor.tableMap;
    }

    @Override
    public Void visit(NamedScan namedScan) {
      List<String> tableName = namedScan.getNames();
      tableMap.put(tableName, namedScan.getInitialSchema());
      return null;
    }

    @Override
    public Void visitFallback(Rel rel) {
      for (Rel input : rel.getInputs()) input.accept(this);
      return null;
    }
  }
}
