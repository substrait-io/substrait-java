package io.substrait.expression.proto;

import io.substrait.expression.ImmutableMaskExpression;
import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpression.ListSelect;
import io.substrait.expression.MaskExpression.ListSelectItem;
import io.substrait.expression.MaskExpression.MapSelect;
import io.substrait.expression.MaskExpression.Select;
import io.substrait.expression.MaskExpression.StructItem;
import io.substrait.expression.MaskExpression.StructSelect;
import io.substrait.proto.Expression;

/**
 * Converts from {@link Expression.MaskExpression} to {@link io.substrait.expression.MaskExpression}
 */
public final class ProtoMaskExpressionConverter {

  private ProtoMaskExpressionConverter() {}

  /** Converts a proto {@link Expression.MaskExpression} to its POJO representation. */
  public static MaskExpression.Mask fromProto(Expression.MaskExpression proto) {
    return MaskExpression.builder()
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
}
