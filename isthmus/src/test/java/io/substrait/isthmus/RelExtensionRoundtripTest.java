package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.Literal;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.relation.Extension;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ImmutableExtensionLeaf;
import io.substrait.relation.ImmutableExtensionMulti;
import io.substrait.relation.ImmutableExtensionSingle;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.ImmutableType;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

class RelExtensionRoundtripTest extends PlanTestBase {
  @Test
  void extensionLeafRelDetailTest() {
    ColumnAppendDetail detail = new ColumnAppendDetail(sb.i32(1));
    ImmutableExtensionLeaf rel = ExtensionLeaf.from(detail).build();
    roundtrip(rel);
  }

  @Test
  void extensionSingleRelDetailTest() {
    ColumnAppendDetail detail = new ColumnAppendDetail(sb.i32(2));
    ImmutableExtensionSingle rel = ExtensionSingle.from(detail, sb.emptyVirtualTableScan()).build();
    roundtrip(rel);
  }

  @Test
  void extensionMultiRelDetailTest() {
    ColumnAppendDetail detail = new ColumnAppendDetail(sb.i32(3));
    ImmutableExtensionMulti rel =
        ExtensionMulti.from(detail, sb.emptyVirtualTableScan(), sb.emptyVirtualTableScan()).build();
    roundtrip(rel);
  }

  void roundtrip(Rel pojo1) {
    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.Rel proto =
        pojo1.accept(
            new RelProtoConverter(new ExtensionCollector()), EmptyVisitationContext.INSTANCE);

    // Substrait Proto -> Substrait POJO 2
    Rel pojo2 = (new CustomProtoRelConverter(new ExtensionCollector())).from(proto);
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelNode calcite =
        pojo2.accept(
            new CustomSubstraitRelNodeConverter(extensions, typeFactory, builder),
            Context.newContext());

    // Calcite -> Substrait POJO 3
    Rel pojo3 = (new CustomSubstraitRelVisitor(typeFactory, extensions)).apply(calcite);
    assertEquals(pojo1, pojo3);
  }

  static class ColumnAppendDetail
      implements Extension.LeafRelDetail, Extension.SingleRelDetail, Extension.MultiRelDetail {
    Expression.Literal literal;

    ColumnAppendDetail(Expression.Literal literal) {
      this.literal = literal;
    }

    @Override
    // LeafRelDetail
    public Type.Struct deriveRecordType() {
      return Type.Struct.builder().nullable(false).addFields(literal.getType()).build();
    }

    @Override
    // SingleRelDetail
    public Type.Struct deriveRecordType(Rel input) {
      return Type.Struct.builder()
          .nullable(false)
          .addAllFields(input.getRecordType().fields())
          .addFields(literal.getType())
          .build();
    }

    @Override
    // MultiRelDetail
    public Type.Struct deriveRecordType(List<Rel> inputs) {
      ImmutableType.Struct.Builder builder = Type.Struct.builder().nullable(false);
      for (Rel input : inputs) {
        builder.addAllFields(input.getRecordType().fields());
      }
      return builder.addFields(literal.getType()).build();
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
      // the conversion of the literal in the detail requires the presence of the RelProtoConverter
      io.substrait.proto.Expression lit =
          converter.getExpressionProtoConverter().toProto(this.literal);
      io.substrait.isthmus.extensions.test.protobuf.ColumnAppendDetail inner =
          io.substrait.isthmus.extensions.test.protobuf.ColumnAppendDetail.newBuilder()
              .setLiteral(lit.getLiteral())
              .build();
      return Any.pack(inner);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      ColumnAppendDetail that = (ColumnAppendDetail) o;
      return Objects.equals(literal, that.literal);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(literal);
    }

    @Override
    public String toString() {
      return "ColumnAppendDetail{" + "literal=" + literal + '}';
    }
  }

  /**
   * Extends the standard {@link ProtoRelConverter} to be able to handle {link
   * io.substrait.isthmus.extensions.test.protobuf.ColumnAppendDetail} messages
   */
  static class CustomProtoRelConverter extends ProtoRelConverter {

    public CustomProtoRelConverter(ExtensionLookup lookup) {
      super(lookup);
    }

    ColumnAppendDetail unpack(Any any) {
      try {
        io.substrait.isthmus.extensions.test.protobuf.ColumnAppendDetail proto =
            any.unpack(io.substrait.isthmus.extensions.test.protobuf.ColumnAppendDetail.class);
        Literal literal =
            (new ProtoExpressionConverter(
                    lookup, extensions, Type.Struct.builder().nullable(false).build(), this)
                .from(proto.getLiteral()));
        return new ColumnAppendDetail(literal);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    protected Extension.LeafRelDetail detailFromExtensionLeafRel(Any any) {
      return unpack(any);
    }

    @Override
    protected Extension.SingleRelDetail detailFromExtensionSingleRel(Any any) {
      return unpack(any);
    }

    @Override
    protected Extension.MultiRelDetail detailFromExtensionMultiRel(Any any) {
      return unpack(any);
    }
  }

  /**
   * Extends the standard {@link SubstraitRelNodeConverter} to handle Extension relations containing
   * {@link ColumnAppendDetail}
   */
  static class CustomSubstraitRelNodeConverter extends SubstraitRelNodeConverter {

    public CustomSubstraitRelNodeConverter(
        SimpleExtension.ExtensionCollection extensions,
        RelDataTypeFactory typeFactory,
        RelBuilder relBuilder) {
      super(extensions, typeFactory, relBuilder);
    }

    @Override
    public RelNode visit(ExtensionLeaf extensionLeaf, Context context) {
      if (extensionLeaf.getDetail() instanceof ColumnAppendDetail) {
        ColumnAppendDetail cad = (ColumnAppendDetail) extensionLeaf.getDetail();
        RexLiteral literal = (RexLiteral) cad.literal.accept(this.expressionRexConverter, context);
        RelOptCluster cluster = relBuilder.getCluster();
        RelTraitSet traits = cluster.traitSet();
        return new ColumnAppenderRel(
            relBuilder.getCluster(), traits, literal, Collections.emptyList());
      }
      throw new UnsupportedOperationException("detail was not ColumnAppendDetail");
    }

    @Override
    public RelNode visit(ExtensionSingle extensionSingle, Context context) throws RuntimeException {
      if (extensionSingle.getDetail() instanceof ColumnAppendDetail) {
        ColumnAppendDetail cad = (ColumnAppendDetail) extensionSingle.getDetail();
        RelNode input = extensionSingle.getInput().accept(this, context);
        RexLiteral literal = (RexLiteral) cad.literal.accept(this.expressionRexConverter, context);
        return new ColumnAppenderRel(
            input.getCluster(), input.getTraitSet(), literal, List.of(input));
      }
      throw new UnsupportedOperationException("detail was not ColumnAppendDetail");
    }

    @Override
    public RelNode visit(ExtensionMulti extensionMulti, Context context) throws RuntimeException {
      if (extensionMulti.getDetail() instanceof ColumnAppendDetail) {
        ColumnAppendDetail cad = (ColumnAppendDetail) extensionMulti.getDetail();
        List<RelNode> inputs =
            extensionMulti.getInputs().stream()
                .map(input -> input.accept(this, context))
                .collect(Collectors.toList());
        RexLiteral literal = (RexLiteral) cad.literal.accept(this.expressionRexConverter, context);
        return new ColumnAppenderRel(
            inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), literal, inputs);
      }
      throw new UnsupportedOperationException("detail was not ColumnAppendDetail");
    }
  }

  /** Extends the standard {@link SubstraitRelVisitor} to handle the {@link ColumnAppenderRel} */
  static class CustomSubstraitRelVisitor extends SubstraitRelVisitor {

    public CustomSubstraitRelVisitor(
        RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
      super(typeFactory, extensions);
    }

    @Override
    public Rel visitOther(RelNode other) {
      if (other instanceof ColumnAppenderRel) {
        ColumnAppenderRel car = (ColumnAppenderRel) other;
        Expression.Literal literal = (Expression.Literal) toExpression(car.literal);
        ColumnAppendDetail detail = new ColumnAppendDetail(literal);
        List<Rel> inputs = apply(car.getInputs());

        if (inputs.isEmpty()) {
          return ExtensionLeaf.from(detail).build();
        } else if (inputs.size() == 1) {
          return ExtensionSingle.from(detail, inputs.get(0)).build();
        } else {
          return ExtensionMulti.from(detail, inputs).build();
        }
      }
      return super.visitOther(other);
    }
  }

  /** Maps to a Substrait Extension {@link Rel} with the {@link ColumnAppendDetail} message set */
  static class ColumnAppenderRel extends AbstractRelNode {

    final RexLiteral literal;
    final List<RelNode> inputs;

    public ColumnAppenderRel(
        RelOptCluster cluster, RelTraitSet traitSet, RexLiteral literal, List<RelNode> inputs) {
      super(cluster, traitSet);
      this.literal = literal;
      this.inputs = inputs;
    }

    @Override
    public List<RelNode> getInputs() {
      return inputs;
    }

    @Override
    protected RelDataType deriveRowType() {
      List<RelDataTypeField> fields = new ArrayList<>();
      for (RelNode input : getInputs()) {
        fields.addAll(input.getRowType().getFieldList());
      }
      RelDataTypeFieldImpl appendedField =
          new RelDataTypeFieldImpl("appended_column", fields.size(), literal.getType());
      fields.add(appendedField);
      return getCluster()
          .getTypeFactory()
          .createStructType(
              fields.stream().map(RelDataTypeField::getType).collect(Collectors.toList()),
              // a real implementation would have to check that names are unique
              fields.stream().map(RelDataTypeField::getName).collect(Collectors.toList()));
    }
  }
}
