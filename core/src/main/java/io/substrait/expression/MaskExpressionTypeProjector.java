package io.substrait.expression;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;

/**
 * Applies a {@link MaskExpression} projection to a {@link Type.Struct}, returning a pruned struct.
 */
public final class MaskExpressionTypeProjector {

  private MaskExpressionTypeProjector() {}

  public static Type.Struct project(MaskExpression projection, Type.Struct baseStruct) {
    return projectStruct(projection.getSelect(), baseStruct);
  }

  private static Type.Struct projectStruct(
      MaskExpression.StructSelect structSelect, Type.Struct baseStruct) {
    List<Type> fields = baseStruct.fields();
    List<MaskExpression.StructItem> items = structSelect.getStructItems();

    return TypeCreator.of(baseStruct.nullable())
        .struct(items.stream().map(item -> projectItem(item, fields.get(item.getField()))));
  }

  private static Type projectItem(MaskExpression.StructItem item, Type fieldType) {
    if (!item.getChild().isPresent()) {
      return fieldType;
    }

    MaskExpression.Select select = item.getChild().get();

    if (select instanceof MaskExpression.StructSelect) {
      return projectStruct((MaskExpression.StructSelect) select, (Type.Struct) fieldType);
    }

    if (select instanceof MaskExpression.ListSelect) {
      return projectList((MaskExpression.ListSelect) select, (Type.ListType) fieldType);
    }

    if (select instanceof MaskExpression.MapSelect) {
      return projectMap((MaskExpression.MapSelect) select, (Type.Map) fieldType);
    }

    return fieldType;
  }

  private static Type.ListType projectList(
      MaskExpression.ListSelect listSelect, Type.ListType listType) {
    if (!listSelect.getChild().isPresent()) {
      return listType;
    }

    MaskExpression.Select childSelect = listSelect.getChild().get();
    Type elementType = listType.elementType();

    if (childSelect instanceof MaskExpression.StructSelect && elementType instanceof Type.Struct) {
      Type.Struct prunedElement =
          projectStruct((MaskExpression.StructSelect) childSelect, (Type.Struct) elementType);
      return TypeCreator.of(listType.nullable()).list(prunedElement);
    }

    return listType;
  }

  private static Type.Map projectMap(MaskExpression.MapSelect mapSelect, Type.Map mapType) {
    if (!mapSelect.getChild().isPresent()) {
      return mapType;
    }

    MaskExpression.Select childSelect = mapSelect.getChild().get();
    Type valueType = mapType.value();

    if (childSelect instanceof MaskExpression.StructSelect && valueType instanceof Type.Struct) {
      Type.Struct prunedValue =
          projectStruct((MaskExpression.StructSelect) childSelect, (Type.Struct) valueType);
      return TypeCreator.of(mapType.nullable()).map(mapType.key(), prunedValue);
    }

    return mapType;
  }
}
