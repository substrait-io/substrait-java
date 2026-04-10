package io.substrait.expression.proto;

import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpression.ListSelect;
import io.substrait.expression.MaskExpression.ListSelectItem;
import io.substrait.expression.MaskExpression.MapSelect;
import io.substrait.expression.MaskExpression.Select;
import io.substrait.expression.MaskExpression.StructItem;
import io.substrait.expression.MaskExpression.StructSelect;
import io.substrait.expression.MaskExpressionVisitor;
import io.substrait.proto.Expression;
import io.substrait.util.EmptyVisitationContext;

/**
 * Converts from {@link io.substrait.expression.MaskExpression} to {@link Expression.MaskExpression}
 */
public final class MaskExpressionProtoConverter {

  private MaskExpressionProtoConverter() {}

  private static final MaskExpressionVisitor<
          Expression.MaskExpression.Select, EmptyVisitationContext, RuntimeException>
      SELECT_TO_PROTO_VISITOR =
          new MaskExpressionVisitor<
              Expression.MaskExpression.Select, EmptyVisitationContext, RuntimeException>() {
            @Override
            public Expression.MaskExpression.Select visit(
                MaskExpression.StructSelect structSelect, EmptyVisitationContext context) {
              return Expression.MaskExpression.Select.newBuilder()
                  .setStruct(toProto(structSelect))
                  .build();
            }

            @Override
            public Expression.MaskExpression.Select visit(
                MaskExpression.ListSelect listSelect, EmptyVisitationContext context) {
              return Expression.MaskExpression.Select.newBuilder()
                  .setList(toProtoListSelect(listSelect))
                  .build();
            }

            @Override
            public Expression.MaskExpression.Select visit(
                MaskExpression.MapSelect mapSelect, EmptyVisitationContext context) {
              return Expression.MaskExpression.Select.newBuilder()
                  .setMap(toProtoMapSelect(mapSelect))
                  .build();
            }
          };

  /**
   * Converts a POJO {@link MaskExpression} to its proto representation.
   *
   * @param mask the POJO {@link MaskExpression}
   * @return the proto {@link Expression.MaskExpression}
   */
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
    return select.accept(SELECT_TO_PROTO_VISITOR, EmptyVisitationContext.INSTANCE);
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
    if (item.getItem().isPresent()) {
      builder.setItem(
          Expression.MaskExpression.ListSelect.ListSelectItem.ListElement.newBuilder()
              .setField(item.getItem().get().getField())
              .build());
    } else if (item.getSlice().isPresent()) {
      builder.setSlice(
          Expression.MaskExpression.ListSelect.ListSelectItem.ListSlice.newBuilder()
              .setStart(item.getSlice().get().getStart())
              .setEnd(item.getSlice().get().getEnd())
              .build());
    } else {
      throw new IllegalArgumentException("ListSelectItem must have either item or slice set");
    }
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
