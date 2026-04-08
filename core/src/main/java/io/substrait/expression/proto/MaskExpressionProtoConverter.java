package io.substrait.expression.proto;

import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpression.ListSelect;
import io.substrait.expression.MaskExpression.ListSelectItem;
import io.substrait.expression.MaskExpression.MapSelect;
import io.substrait.expression.MaskExpression.Select;
import io.substrait.expression.MaskExpression.StructItem;
import io.substrait.expression.MaskExpression.StructSelect;
import io.substrait.proto.Expression;

/**
 * Converts from {@link io.substrait.expression.MaskExpression} to {@link Expression.MaskExpression}
 */
public final class MaskExpressionProtoConverter {

  private MaskExpressionProtoConverter() {}

  /** Converts a POJO {@link MaskExpression} to its proto representation. */
  public static Expression.MaskExpression toProto(MaskExpression mask) {
    return Expression.MaskExpression.newBuilder()
        .setSelect(toProto(mask.getSelect()))
        .setMaintainSingularStruct(mask.getMaintainSingularStruct())
        .build();
  }

  private static Expression.MaskExpression.StructSelect toProto(StructSelect structSelect) {
    Expression.MaskExpression.StructSelect.Builder builder =
        Expression.MaskExpression.StructSelect.newBuilder();
    for (StructItem item : structSelect.getStructItems()) {
      builder.addStructItems(toProto(item));
    }
    return builder.build();
  }

  private static Expression.MaskExpression.StructItem toProto(StructItem structItem) {
    Expression.MaskExpression.StructItem.Builder builder =
        Expression.MaskExpression.StructItem.newBuilder().setField(structItem.getField());
    structItem.getChild().ifPresent(child -> builder.setChild(toProtoSelect(child)));
    return builder.build();
  }

  private static Expression.MaskExpression.Select toProtoSelect(Select select) {
    Expression.MaskExpression.Select.Builder builder =
        Expression.MaskExpression.Select.newBuilder();
    if (select instanceof MaskExpression.StructSelect) {
      builder.setStruct(toProto((MaskExpression.StructSelect) select));
    } else if (select instanceof MaskExpression.ListSelect) {
      builder.setList(toProtoListSelect((MaskExpression.ListSelect) select));
    } else if (select instanceof MaskExpression.MapSelect) {
      builder.setMap(toProtoMapSelect((MaskExpression.MapSelect) select));
    }
    return builder.build();
  }

  private static Expression.MaskExpression.ListSelect toProtoListSelect(ListSelect listSelect) {
    Expression.MaskExpression.ListSelect.Builder builder =
        Expression.MaskExpression.ListSelect.newBuilder();
    for (ListSelectItem item : listSelect.getSelection()) {
      builder.addSelection(toProtoListSelectItem(item));
    }
    listSelect.getChild().ifPresent(child -> builder.setChild(toProtoSelect(child)));
    return builder.build();
  }

  private static Expression.MaskExpression.ListSelect.ListSelectItem toProtoListSelectItem(
      ListSelectItem item) {
    Expression.MaskExpression.ListSelect.ListSelectItem.Builder builder =
        Expression.MaskExpression.ListSelect.ListSelectItem.newBuilder();
    item.getItem()
        .ifPresent(
            element ->
                builder.setItem(
                    Expression.MaskExpression.ListSelect.ListSelectItem.ListElement.newBuilder()
                        .setField(element.getField())
                        .build()));
    item.getSlice()
        .ifPresent(
            slice ->
                builder.setSlice(
                    Expression.MaskExpression.ListSelect.ListSelectItem.ListSlice.newBuilder()
                        .setStart(slice.getStart())
                        .setEnd(slice.getEnd())
                        .build()));
    return builder.build();
  }

  private static Expression.MaskExpression.MapSelect toProtoMapSelect(MapSelect mapSelect) {
    Expression.MaskExpression.MapSelect.Builder builder =
        Expression.MaskExpression.MapSelect.newBuilder();
    mapSelect
        .getKey()
        .ifPresent(
            key ->
                builder.setKey(
                    Expression.MaskExpression.MapSelect.MapKey.newBuilder()
                        .setMapKey(key.getMapKey())
                        .build()));
    mapSelect
        .getExpression()
        .ifPresent(
            expr ->
                builder.setExpression(
                    Expression.MaskExpression.MapSelect.MapKeyExpression.newBuilder()
                        .setMapKeyExpression(expr.getMapKeyExpression())
                        .build()));
    mapSelect.getChild().ifPresent(child -> builder.setChild(toProtoSelect(child)));
    return builder.build();
  }
}
