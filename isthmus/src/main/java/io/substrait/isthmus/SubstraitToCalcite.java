package io.substrait.isthmus;

import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

/**
 * Converts between Substrait {@link Rel}s and Calcite {@link RelNode}s.
 *
 * <p>Conversion behaviours can be customized using a {@link ConverterProvider}
 */
public class SubstraitToCalcite {

  protected final RelDataTypeFactory typeFactory;
  protected final Prepare.CatalogReader catalogReader;
  protected ConverterProvider converterProvider;

  public SubstraitToCalcite(ConverterProvider converterProvider) {
    this(converterProvider, null);
  }

  public SubstraitToCalcite(
      ConverterProvider converterProvider, Prepare.CatalogReader catalogReader) {
    this.converterProvider = converterProvider;
    this.typeFactory = converterProvider.getTypeFactory();
    this.catalogReader = catalogReader;
  }

  /**
   * Converts a Substrait {@link Rel} to a Calcite {@link RelNode}
   *
   * @param rel {@link Rel} to convert
   * @return {@link RelNode}
   */
  public RelNode convert(Rel rel) {
    RelBuilder relBuilder;
    if (catalogReader != null) {
      relBuilder = converterProvider.getRelBuilder(catalogReader.getRootSchema());
    } else {
      CalciteSchema rootSchema = converterProvider.getSchemaResolver().apply(rel);
      relBuilder = converterProvider.getRelBuilder(rootSchema);
    }
    SubstraitRelNodeConverter converter =
        converterProvider.getSubstraitRelNodeConverter(relBuilder);

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
  public RelRoot convert(Plan.Root root) {
    RelNode convertedNode = convert(root.getInput());

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
    SqlKindFromRel sqlKindFromRel = new SqlKindFromRel();
    SqlKind kind = root.getInput().accept(sqlKindFromRel, EmptyVisitationContext.INSTANCE);
    RelDataType inputRowType = convertedNode.getRowType();
    RelDataType newRowType = renameFields(inputRowType, root.getNames(), 0).right;
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
}
