package io.substrait.expression;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
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

    return select.accept(
        new MaskExpressionVisitor<Type, EmptyVisitationContext, RuntimeException>() {
          @Override
          public Type visit(
              MaskExpression.StructSelect structSelect, EmptyVisitationContext context) {
            return projectStruct(structSelect, (Type.Struct) fieldType);
          }

          @Override
          public Type visit(MaskExpression.ListSelect listSelect, EmptyVisitationContext context) {
            return projectList(listSelect, (Type.ListType) fieldType);
          }

          @Override
          public Type visit(MaskExpression.MapSelect mapSelect, EmptyVisitationContext context) {
            return projectMap(mapSelect, (Type.Map) fieldType);
          }
        },
        EmptyVisitationContext.INSTANCE);
  }

  private static Type.ListType projectList(
      MaskExpression.ListSelect listSelect, Type.ListType listType) {
    if (!listSelect.getChild().isPresent()) {
      return listType;
    }

    MaskExpression.Select childSelect = listSelect.getChild().get();
    Type elementType = listType.elementType();

    return childSelect.accept(
        new MaskExpressionVisitor<Type.ListType, EmptyVisitationContext, RuntimeException>() {
          @Override
          public Type.ListType visit(
              MaskExpression.StructSelect structSelect, EmptyVisitationContext context) {
            if (elementType instanceof Type.Struct) {
              Type.Struct prunedElement = projectStruct(structSelect, (Type.Struct) elementType);
              return TypeCreator.of(listType.nullable()).list(prunedElement);
            }
            return listType;
          }

          @Override
          public Type.ListType visit(
              MaskExpression.ListSelect listSelect, EmptyVisitationContext context) {
            return listType;
          }

          @Override
          public Type.ListType visit(
              MaskExpression.MapSelect mapSelect, EmptyVisitationContext context) {
            return listType;
          }
        },
        EmptyVisitationContext.INSTANCE);
  }

  private static Type.Map projectMap(MaskExpression.MapSelect mapSelect, Type.Map mapType) {
    if (!mapSelect.getChild().isPresent()) {
      return mapType;
    }

    MaskExpression.Select childSelect = mapSelect.getChild().get();
    Type valueType = mapType.value();

    return childSelect.accept(
        new MaskExpressionVisitor<Type.Map, EmptyVisitationContext, RuntimeException>() {
          @Override
          public Type.Map visit(
              MaskExpression.StructSelect structSelect, EmptyVisitationContext context) {
            if (valueType instanceof Type.Struct) {
              Type.Struct prunedValue = projectStruct(structSelect, (Type.Struct) valueType);
              return TypeCreator.of(mapType.nullable()).map(mapType.key(), prunedValue);
            }
            return mapType;
          }

          @Override
          public Type.Map visit(
              MaskExpression.ListSelect listSelect, EmptyVisitationContext context) {
            return mapType;
          }

          @Override
          public Type.Map visit(
              MaskExpression.MapSelect mapSelect, EmptyVisitationContext context) {
            return mapType;
          }
        },
        EmptyVisitationContext.INSTANCE);
  }
}
