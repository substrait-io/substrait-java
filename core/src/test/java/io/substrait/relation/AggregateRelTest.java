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
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class AggregateRelTest extends TestBase {

  protected static final Plan plan = Plan.newBuilder().build();
  protected static final ExtensionLookup functionLookup =
      ImmutableExtensionLookup.builder().from(plan).build();
  protected static final io.substrait.proto.NamedStruct namedStruct = createSchema();

  public static io.substrait.proto.NamedStruct createSchema() {

    io.substrait.proto.Type i32Type =
        io.substrait.proto.Type.newBuilder()
            .setI32(io.substrait.proto.Type.I32.getDefaultInstance())
            .build();

    // Build a NamedStruct schema with two fields: col1, col2
    io.substrait.proto.Type.Struct structType =
        io.substrait.proto.Type.Struct.newBuilder().addTypes(i32Type).addTypes(i32Type).build();

    return io.substrait.proto.NamedStruct.newBuilder()
        .setStruct(structType)
        .addNames("col1")
        .addNames("col2")
        .build();
  }

  public static io.substrait.proto.Expression createFieldReference(int col) {
    // Build a ReferenceSegment that refers to struct field col
    Expression.ReferenceSegment seg1 =
        Expression.ReferenceSegment.newBuilder()
            .setStructField(
                Expression.ReferenceSegment.StructField.newBuilder().setField(col).build())
            .build();

    // Build a FieldReference that uses the directReference and a rootReference
    Expression.FieldReference fieldRef1 =
        Expression.FieldReference.newBuilder()
            .setDirectReference(seg1)
            .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance())
            .build();

    // Wrap the FieldReference in an Expression.selection
    return Expression.newBuilder().setSelection(fieldRef1).build();
  }

  /**
   * Helper method to extract expression references from an AggregateRel.
   *
   * @param aggregateRel the AggregateRel to extract expression references from
   * @return a list of lists, where each inner list contains the expression reference indices for a
   *     grouping
   */
  private static List<List<Integer>> getExpressionReferences(AggregateRel aggregateRel) {
    return aggregateRel.getGroupingsList().stream()
        .map(
            grouping ->
                grouping.getExpressionReferencesList().stream().collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  /**
   * Helper method to extract deprecated grouping expressions from an AggregateRel.
   *
   * @param aggregateRel the AggregateRel to extract grouping expressions from
   * @return a list of lists, where each inner list contains the grouping expressions for a grouping
   */
  private static List<List<Expression>> getGroupingExpressions(AggregateRel aggregateRel) {
    return aggregateRel.getGroupingsList().stream()
        .map(grouping -> grouping.getGroupingExpressionsList())
        .collect(Collectors.toList());
  }

  /**
   * Helper method to extract aggregate-level grouping expressions from an AggregateRel.
   *
   * @param aggregateRel the AggregateRel to extract grouping expressions from
   * @return a list of expressions at the aggregate level
   */
  private static List<Expression> getAggregateGroupingExpressions(AggregateRel aggregateRel) {
    return aggregateRel.getGroupingExpressionsList();
  }

  @Test
  void testDeprecatedGroupingExpressionConversion() {
    Expression col1Ref = createFieldReference(0);
    Expression col2Ref = createFieldReference(1);

    AggregateRel.Grouping grouping =
        AggregateRel.Grouping.newBuilder()
            .addGroupingExpressions(col1Ref) // deprecated proto form
            .addGroupingExpressions(col2Ref)
            .build();

    // Build an input ReadRel
    ReadRel readProto =
        ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
            .setBaseSchema(namedStruct)
            .build();

    // Build the AggregateRel with the new grouping_expressions field
    AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupings(grouping)
            .build();

    Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    Aggregate agg = (Aggregate) resultRel;
    assertEquals(1, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());

    Rel roundtripRel = relProtoConverter.visit(agg, EmptyVisitationContext.INSTANCE);
    assertTrue(roundtripRel.hasAggregate());

    AggregateRel roundtripAgg = roundtripRel.getAggregate();
    // Verify new expression_references structure
    assertEquals(List.of(List.of(0, 1)), getExpressionReferences(roundtripAgg));
    // Verify backward compatibility: deprecated grouping_expressions field is also populated
    assertEquals(List.of(List.of(col1Ref, col2Ref)), getGroupingExpressions(roundtripAgg));
    // Verify aggregate-level grouping_expressions field is populated
    assertEquals(List.of(col1Ref, col2Ref), getAggregateGroupingExpressions(roundtripAgg));
  }

  @Test
  void testAggregateWithSingleGrouping() {
    Expression col1Ref = createFieldReference(0);
    Expression col2Ref = createFieldReference(1);

    AggregateRel.Grouping grouping =
        AggregateRel.Grouping.newBuilder()
            .addExpressionReferences(0)
            .addExpressionReferences(1)
            .build();

    // Build an input ReadRel
    ReadRel readProto =
        ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
            .setBaseSchema(namedStruct)
            .build();

    // Build the AggregateRel with the new grouping_expressions field
    AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupingExpressions(col1Ref)
            .addGroupingExpressions(col2Ref)
            .addGroupings(grouping)
            .build();

    Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    Aggregate agg = (Aggregate) resultRel;
    assertEquals(1, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());

    Rel roundtripRel = relProtoConverter.visit(agg, EmptyVisitationContext.INSTANCE);
    assertTrue(roundtripRel.hasAggregate());

    AggregateRel roundtripAgg = roundtripRel.getAggregate();
    // Verify new expression_references structure
    assertEquals(List.of(List.of(0, 1)), getExpressionReferences(roundtripAgg));
    // Verify backward compatibility: deprecated grouping_expressions field is also populated
    assertEquals(List.of(List.of(col1Ref, col2Ref)), getGroupingExpressions(roundtripAgg));
    // Verify aggregate-level grouping_expressions field is populated
    assertEquals(List.of(col1Ref, col2Ref), getAggregateGroupingExpressions(roundtripAgg));
  }

  @Test
  void testAggregateWithMultipleGroupings() {
    Expression col1Ref = createFieldReference(0);
    Expression col2Ref = createFieldReference(1);

    AggregateRel.Grouping grouping1 =
        AggregateRel.Grouping.newBuilder()
            .addExpressionReferences(0) // new proto form
            .addExpressionReferences(1)
            .build();

    AggregateRel.Grouping grouping2 =
        AggregateRel.Grouping.newBuilder().addExpressionReferences(1).build();

    // Build an input ReadRel
    ReadRel readProto =
        ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
            .setBaseSchema(namedStruct)
            .build();

    // Build the AggregateRel with the new grouping_expressions field
    AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readProto))
            .addGroupingExpressions(col1Ref)
            .addGroupingExpressions(col2Ref)
            .addGroupings(grouping1)
            .addGroupings(grouping2)
            .build();

    Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
    ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
    io.substrait.relation.Rel resultRel = converter.from(relProto);

    assertTrue(resultRel instanceof Aggregate);
    Aggregate agg = (Aggregate) resultRel;
    assertEquals(2, agg.getGroupings().size());
    assertEquals(2, agg.getGroupings().get(0).getExpressions().size());
    assertEquals(1, agg.getGroupings().get(1).getExpressions().size());

    Rel roundtripRel = relProtoConverter.visit(agg, EmptyVisitationContext.INSTANCE);
    assertTrue(roundtripRel.hasAggregate());

    AggregateRel roundtripAgg = roundtripRel.getAggregate();
    // Verify new expression_references structure
    assertEquals(List.of(List.of(0, 1), List.of(1)), getExpressionReferences(roundtripAgg));
    // Verify backward compatibility: deprecated grouping_expressions field is also populated
    assertEquals(
        List.of(List.of(col1Ref, col2Ref), List.of(col2Ref)), getGroupingExpressions(roundtripAgg));
    // Verify aggregate-level grouping_expressions field is populated
    assertEquals(List.of(col1Ref, col2Ref), getAggregateGroupingExpressions(roundtripAgg));
  }
}
