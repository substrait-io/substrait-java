package io.substrait.expression.proto;

import io.substrait.expression.ImmutableMaskExpression;
import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpression.ListSelect;
import io.substrait.expression.MaskExpression.ListSelectItem;
import io.substrait.expression.MaskExpression.MapSelect;
import io.substrait.expression.MaskExpression.MaskExpr;
import io.substrait.expression.MaskExpression.Select;
import io.substrait.expression.MaskExpression.StructItem;
import io.substrait.expression.MaskExpression.StructSelect;
import io.substrait.proto.Expression;

/** Converts between proto {@link Expression.MaskExpression} and POJO {@link MaskExpression}. */
public final class MaskExpressionProtoConverter {

  private MaskExpressionProtoConverter() {}

  // ---------------------------------------------------------------------------
  // Proto -> POJO
  // ---------------------------------------------------------------------------

  public static MaskExpr fromProto(Expression.MaskExpression proto) {
    return MaskExpr.builder()
        .select(fromProto(proto.getSelect()))
        .maintainSingularStruct(proto.getMaintainSingularStruct())
        .build();
  }

  private static StructSelect fromProto(Expression.MaskExpression.StructSelect proto) {
    ImmutableMaskExpression.StructSelect.Builder builder = StructSelect.builder();
    for (Expression.MaskExpression.StructItem item : proto.getStructItemsList()) {
      builder.addStructItems(fromProto(item));
    }
    return builder.build();
  }

  private static StructItem fromProto(Expression.MaskExpression.StructItem proto) {
    ImmutableMaskExpression.StructItem.Builder builder =
        StructItem.builder().field(proto.getField());
    if (proto.hasChild()) {
      builder.child(fromProtoSelect(proto.getChild()));
    }
    return builder.build();
  }

  private static Select fromProtoSelect(Expression.MaskExpression.Select proto) {
    switch (proto.getTypeCase()) {
      case STRUCT:
        return fromProto(proto.getStruct());
      case LIST:
        return fromProtoListSelect(proto.getList());
      case MAP:
        return fromProtoMapSelect(proto.getMap());
      default:
        throw new IllegalArgumentException(
            "Unknown MaskExpression.Select type: " + proto.getTypeCase());
    }
  }

  private static ListSelect fromProtoListSelect(Expression.MaskExpression.ListSelect proto) {
    ImmutableMaskExpression.ListSelect.Builder builder = ListSelect.builder();
    for (Expression.MaskExpression.ListSelect.ListSelectItem item : proto.getSelectionList()) {
      builder.addSelection(fromProtoListSelectItem(item));
    }
    if (proto.hasChild()) {
      builder.child(fromProtoSelect(proto.getChild()));
    }
    return builder.build();
  }

  private static ListSelectItem fromProtoListSelectItem(
      Expression.MaskExpression.ListSelect.ListSelectItem proto) {
    ImmutableMaskExpression.ListSelectItem.Builder builder = ListSelectItem.builder();
    switch (proto.getTypeCase()) {
      case ITEM:
        builder.item(MaskExpression.ListElement.of(proto.getItem().getField()));
        break;
      case SLICE:
        builder.slice(
            MaskExpression.ListSlice.of(proto.getSlice().getStart(), proto.getSlice().getEnd()));
        break;
      default:
        throw new IllegalArgumentException("Unknown ListSelectItem type: " + proto.getTypeCase());
    }
    return builder.build();
  }

  private static MapSelect fromProtoMapSelect(Expression.MaskExpression.MapSelect proto) {
    ImmutableMaskExpression.MapSelect.Builder builder = MapSelect.builder();
    switch (proto.getSelectCase()) {
      case KEY:
        builder.key(MaskExpression.MapKey.of(proto.getKey().getMapKey()));
        break;
      case EXPRESSION:
        builder.expression(
            MaskExpression.MapKeyExpression.of(proto.getExpression().getMapKeyExpression()));
        break;
      default:
        throw new IllegalArgumentException("Unknown MapSelect type: " + proto.getSelectCase());
    }
    if (proto.hasChild()) {
      builder.child(fromProtoSelect(proto.getChild()));
    }
    return builder.build();
  }

  // ---------------------------------------------------------------------------
  // POJO -> Proto
  // ---------------------------------------------------------------------------

  public static Expression.MaskExpression toProto(MaskExpr mask) {
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
