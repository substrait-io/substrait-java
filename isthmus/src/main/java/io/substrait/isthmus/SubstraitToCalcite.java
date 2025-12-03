package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.NamedStruct;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
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
  protected final Prepare.CatalogReader catalogReader;

  public SubstraitToCalcite(
      final SimpleExtension.ExtensionCollection extensions, final RelDataTypeFactory typeFactory) {
    this(extensions, typeFactory, TypeConverter.DEFAULT, null);
  }

  public SubstraitToCalcite(
      final SimpleExtension.ExtensionCollection extensions,
      final RelDataTypeFactory typeFactory,
      final Prepare.CatalogReader catalogReader) {
    this(extensions, typeFactory, TypeConverter.DEFAULT, catalogReader);
  }

  public SubstraitToCalcite(
      final SimpleExtension.ExtensionCollection extensions,
      final RelDataTypeFactory typeFactory,
      final TypeConverter typeConverter) {
    this(extensions, typeFactory, typeConverter, null);
  }

  public SubstraitToCalcite(
      final SimpleExtension.ExtensionCollection extensions,
      final RelDataTypeFactory typeFactory,
      final TypeConverter typeConverter,
      final Prepare.CatalogReader catalogReader) {
    this.extensions = extensions;
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.catalogReader = catalogReader;
  }

  /**
   * Extracts a {@link CalciteSchema} from a {@link Rel}
   *
   * <p>Override this method to customize schema extraction.
   */
  protected CalciteSchema toSchema(final Rel rel) {
    final SchemaCollector schemaCollector = new SchemaCollector(typeFactory, typeConverter);
    return schemaCollector.toSchema(rel);
  }

  /**
   * Creates a {@link RelBuilder} from the extracted {@link CalciteSchema}
   *
   * <p>Override this method to customize the {@link RelBuilder}.
   */
  protected RelBuilder createRelBuilder(final CalciteSchema schema) {
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema.plus()).build());
  }

  /**
   * Creates a {@link SubstraitRelNodeConverter} from the {@link RelBuilder}
   *
   * <p>Override this method to customize the {@link SubstraitRelNodeConverter}.
   */
  protected SubstraitRelNodeConverter createSubstraitRelNodeConverter(final RelBuilder relBuilder) {
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
  public RelNode convert(final Rel rel) {
    final RelBuilder relBuilder;
    if (catalogReader != null) {
      relBuilder = createRelBuilder(catalogReader.getRootSchema());
    } else {
      final CalciteSchema rootSchema = toSchema(rel);
      relBuilder = createRelBuilder(rootSchema);
    }
    final SubstraitRelNodeConverter converter = createSubstraitRelNodeConverter(relBuilder);

    return rel.accept(converter, Context.newContext());
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
  public RelRoot convert(final Plan.Root root) {
    final RelNode convertedNode = convert(root.getInput());

    if (convertedNode instanceof TableModify) {
      final TableModify tableModify = (TableModify) convertedNode;
      final RelDataType tableRowType = tableModify.getTable().getRowType();
      final SqlKind kind;

      switch (tableModify.getOperation()) {
        case INSERT:
          kind = SqlKind.INSERT;
          break;
        case UPDATE:
          kind = SqlKind.UPDATE;
          break;
        case DELETE:
          kind = SqlKind.DELETE;
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported table modify operation: " + tableModify.getOperation());
      }
      return RelRoot.of(tableModify, tableRowType, kind);
    }
    final SqlKindFromRel sqlKindFromRel = new SqlKindFromRel();
    final SqlKind kind = root.getInput().accept(sqlKindFromRel, EmptyVisitationContext.INSTANCE);
    final RelDataType inputRowType = convertedNode.getRowType();
    final RelDataType newRowType = renameFields(inputRowType, root.getNames(), 0).right;
    return RelRoot.of(convertedNode, newRowType, kind);
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
      final RelDataType type, final List<String> names, final Integer currentIndex) {
    Integer nextIndex = currentIndex;

    switch (type.getSqlTypeName()) {
      case ROW:
      case STRUCTURED:
        final List<String> newFieldNames = new ArrayList<>();
        final List<RelDataType> renamedFields = new ArrayList<>();
        for (final RelDataTypeField field : type.getFieldList()) {
          newFieldNames.add(names.get(nextIndex));
          final Pair<Integer, RelDataType> p =
              renameFields(field.getType(), names, (nextIndex + 1));
          renamedFields.add(p.right);
          nextIndex = p.left;
        }

        return Pair.of(
            nextIndex,
            typeFactory.createStructType(type.getStructKind(), renamedFields, newFieldNames));
      case ARRAY:
      case MULTISET:
        final Pair<Integer, RelDataType> renamedElementType =
            renameFields(type.getComponentType(), names, nextIndex);

        return Pair.of(
            renamedElementType.left, typeFactory.createArrayType(renamedElementType.right, -1L));
      case MAP:
        final Pair<Integer, RelDataType> renamedKeyType =
            renameFields(type.getKeyType(), names, nextIndex);
        final Pair<Integer, RelDataType> renamedValueType =
            renameFields(type.getValueType(), names, renamedKeyType.left);

        return Pair.of(
            renamedValueType.left,
            typeFactory.createMapType(renamedKeyType.right, renamedValueType.right));
      default:
        return Pair.of(currentIndex, type);
    }
  }

  private static class NamedStructGatherer extends RelCopyOnWriteVisitor<RuntimeException> {
    Map<List<String>, NamedStruct> tableMap;

    private NamedStructGatherer() {
      super();
      this.tableMap = new HashMap<>();
    }

    public static Map<List<String>, NamedStruct> gatherTables(final Rel rel) {
      final NamedStructGatherer visitor = new NamedStructGatherer();
      rel.accept(visitor, EmptyVisitationContext.INSTANCE);
      return visitor.tableMap;
    }

    @Override
    public Optional<Rel> visit(final NamedScan namedScan, final EmptyVisitationContext context) {
      final Optional<Rel> result = super.visit(namedScan, context);

      final List<String> tableName = namedScan.getNames();
      tableMap.put(tableName, namedScan.getInitialSchema());

      return result;
    }
  }
}
