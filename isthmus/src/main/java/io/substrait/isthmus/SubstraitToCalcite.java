package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

/**
 * Converts between Substrait {@link Rel}s and Calcite {@link RelNode}s.
 *
 * <p>Can be extended to customize the {@link RelBuilder} and {@link SubstraitRelNodeConverter} used
 * in the conversion.
 */
public class SubstraitToCalcite {

  protected final SimpleExtension.ExtensionCollection extensions;
  protected final RelDataTypeFactory typeFactory;
  protected final TypeConverter typeConverter;

  public SubstraitToCalcite(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    this(extensions, typeFactory, TypeConverter.DEFAULT);
  }

  public SubstraitToCalcite(
      SimpleExtension.ExtensionCollection extensions,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    this.extensions = extensions;
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
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
              typeConverter.toCalcite(typeFactory, table.struct(), table.names()));
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

  /**
   * Converts a Substrait {@link Plan.Root} to a Calcite {@link RelRoot}
   *
   * <p>Generates a {@link RelDataType} row type with the final field names of the {@link Plan.Root}
   * and creates a Calcite {@link RelRoot} with it.
   *
   * <p>TODO: revisit this code when support for WriteRel is added to substrait-java
   *
   * <p>TODO: this code assumes that the Apache Calcite knows how to properly alias hierarchical
   * field names which is currently not the case (Calcite version 1.39.0)
   *
   * @param root {@link Plan.Root} to convert
   * @return {@link RelRoot}
   */
  public RelRoot convert(Plan.Root root) {
    RelNode input = convert(root.getInput());
    RelDataType inputRowType = input.getRowType();

    RelDataType newRowType = renameFields(inputRowType, root.getNames(), 0).right;
    RelRoot calciteRoot = RelRoot.of(input, newRowType, SqlKind.SELECT);

    return calciteRoot;
  }

  /**
   * Produces a new {@link RelDataType} from the given {@link RelDataType} by recursively applying
   * the given names in depth-first order.
   *
   * @param type the source {@link RelDataType} to rename
   * @param names the names to use for renaming
   * @param currentIndex the current index within the list of names
   * @return the renamed {@link RelDataType}
   */
  private Pair<Integer, RelDataType> renameFields(
      RelDataType type, List<String> names, Integer currentIndex) {
    Integer nextIndex = currentIndex;

    switch (type.getSqlTypeName()) {
      case ROW:
      case STRUCTURED:
        final List<String> newFieldNames = new ArrayList<>();
        final List<RelDataType> renamedFields = new ArrayList<>();
        for (RelDataTypeField field : type.getFieldList()) {
          newFieldNames.add(names.get(nextIndex));
          Pair<Integer, RelDataType> p = renameFields(field.getType(), names, (nextIndex + 1));
          renamedFields.add(p.right);
          nextIndex = p.left;
        }

        return Pair.of(
            nextIndex,
            typeFactory.createStructType(type.getStructKind(), renamedFields, newFieldNames));
      case ARRAY:
      case MULTISET:
        Pair<Integer, RelDataType> renamedElementType =
            renameFields(type.getComponentType(), names, nextIndex);

        return Pair.of(
            renamedElementType.left, typeFactory.createArrayType(renamedElementType.right, -1L));
      case MAP:
        Pair<Integer, RelDataType> renamedKeyType =
            renameFields(type.getKeyType(), names, nextIndex);
        Pair<Integer, RelDataType> renamedValueType =
            renameFields(type.getValueType(), names, renamedKeyType.left);

        return Pair.of(
            renamedValueType.left,
            typeFactory.createMapType(renamedKeyType.right, renamedValueType.right));
      default:
        return Pair.of(currentIndex, type);
    }
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
