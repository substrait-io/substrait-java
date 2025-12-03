package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import org.junit.jupiter.api.Test;

class AggregateRelTest extends TestBase {

  protected static final Plan plan = Plan.newBuilder().build();
  protected static final ExtensionLookup functionLookup =
      ImmutableExtensionLookup.builder().from(plan).build();
  protected static final io.substrait.proto.NamedStruct namedStruct = createSchema();

  public static io.substrait.proto.NamedStruct createSchema() {

    final io.substrait.proto.Type i32Type =
        io.substrait.proto.Type.newBuilder()
            .setI32(io.substrait.proto.Type.I32.getDefaultInstance())
            .build();

    // Build a NamedStruct schema with two fields: col1, col2
    final io.substrait.proto.Type.Struct structType =
        io.substrait.proto.Type.Struct.newBuilder().addTypes(i32Type).addTypes(i32Type).build();

    return io.substrait.proto.NamedStruct.newBuilder()
        .setStruct(structType)
        .addNames("col1")
        .addNames("col2")
        .build();
  }

  public static io.substrait.proto.Expression createFieldReference(final int col) {
    // Build a ReferenceSegment that refers to struct field col
    final Expression.ReferenceSegment seg1 =
        Expression.ReferenceSegment.newBuilder()
            .setStructField(
                Expression.ReferenceSegment.StructField.newBuilder().setField(col).build())
            .build();

    // Build a FieldReference that uses the directReference and a rootReference
    final Expression.FieldReference fieldRef1 =
        Expression.FieldReference.newBuilder()
            .setDirectReference(seg1)
            .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
            .build();

    // Wrap the FieldReference in an Expression.selection
    return Expression.newBuilder().setSelection(fieldRef1).build();
  }

  @Test
  void testDeprecatedGroupingExpressionConversion() {
    final Expression col1Ref = createFieldReference(0);
    final Expression col2Ref = createFieldReference(1);

    final AggregateRel.Grouping grouping =
        AggregateRel.Grouping.newBuilder()
            .addGroupingExpressions(col1Ref) // deprecated proto form
            .addGroupingExpressions(col2Ref)
            .build();

    // Build an input ReadRel
    final ReadRel readProto = ReadRel.newBuilder().setBaseSchema(namedStruct).build();

    // Build the AggregateRel with the new grouping_expressions field
    final AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupings(grouping)
            .build();

    final Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    final ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    final io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    final Aggregate agg = (Aggregate) resultRel;
    assertEquals(1, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());
  }

  @Test
  void testAggregateWithSingleGrouping() {
    final Expression col1Ref = createFieldReference(0);
    final Expression col2Ref = createFieldReference(1);

    final AggregateRel.Grouping grouping =
        AggregateRel.Grouping.newBuilder()
            .addExpressionReferences(0)
            .addExpressionReferences(1)
            .build();

    // Build an input ReadRel
    final ReadRel readProto = ReadRel.newBuilder().setBaseSchema(namedStruct).build();

    // Build the AggregateRel with the new grouping_expressions field
    final AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupingExpressions(col1Ref)
            .addGroupingExpressions(col2Ref)
            .addGroupings(grouping)
            .build();

    final Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    final ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    final io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    final Aggregate agg = (Aggregate) resultRel;
    assertEquals(1, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());
  }

  @Test
  void testAggregateWithMultipleGroupings() {
    final Expression col1Ref = createFieldReference(0);
    final Expression col2Ref = createFieldReference(1);

    final AggregateRel.Grouping grouping1 =
        AggregateRel.Grouping.newBuilder()
            .addExpressionReferences(0) // new proto form
            .addExpressionReferences(1)
            .build();

    final AggregateRel.Grouping grouping2 =
        AggregateRel.Grouping.newBuilder().addExpressionReferences(1).build();

    // Build an input ReadRel
    final ReadRel readProto = ReadRel.newBuilder().setBaseSchema(namedStruct).build();

    // Build the AggregateRel with the new grouping_expressions field
    final AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupingExpressions(col1Ref)
            .addGroupingExpressions(col2Ref)
            .addGroupings(grouping1)
            .addGroupings(grouping2)
            .build();

    final Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    final ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    final io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    final Aggregate agg = (Aggregate) resultRel;
    assertEquals(2, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());
    assertEquals(1, agg.getGroupings().get(1).getExpressions().size());
  }
}
