package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class AggregateRelTest extends TestBase {

  protected static final Plan plan = Plan.newBuilder().build();
  protected static final ExtensionLookup functionLookup =
          ImmutableExtensionLookup.builder().from(plan).build();
    protected static final io.substrait.proto.NamedStruct  namedStruct = createSchema();

    public static io.substrait.proto.NamedStruct  createSchema() {

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

    public static io.substrait.proto.Expression createExpression(int col) {
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


    @Test
  public void testDeprecatedGroupingExpressionsAreMapped() {
      Expression col1Ref = createExpression(0);
      Expression col2Ref = createExpression(1);

      AggregateRel.Grouping grouping =
              AggregateRel.Grouping.newBuilder()
                      .addGroupingExpressions(col1Ref) // deprecated proto form
                      .addGroupingExpressions(col2Ref)
                      .build();

      // Build an input ReadRel
      io.substrait.proto.ReadRel readProto =
              io.substrait.proto.ReadRel.newBuilder().setBaseSchema(namedStruct).build();

      // Build the AggregateRel with the new grouping_expressions field
      AggregateRel aggrProto =
              AggregateRel.newBuilder()
                      .setInput(io.substrait.proto.Rel.newBuilder().setRead(readProto))
                      .addGroupings(grouping)
                      .build();

      Rel relProto = Rel.newBuilder().setAggregate(aggrProto).build();
      ProtoRelConverter converter = new ProtoRelConverter(functionLookup);
      io.substrait.relation.Rel resultRel = converter.from(relProto);

      assertTrue(resultRel instanceof Aggregate);
      Aggregate agg = (Aggregate) resultRel;
      assertEquals(1, agg.getGroupings().size());
      assertEquals(2, agg.getGroupings().get(0).getExpressions().size());
  }

  @Test
  public void testNewAggregateProtoForm() {
      Expression col1Ref = createExpression(0);
      Expression col2Ref = createExpression(1);

      AggregateRel.Grouping grouping =
        AggregateRel.Grouping.newBuilder()
            .addExpressionReferences(0) // new proto form
            .addExpressionReferences(1)
            .build();

    // Build an input ReadRel
      io.substrait.proto.ReadRel readProto =
        io.substrait.proto.ReadRel.newBuilder().setBaseSchema(namedStruct).build();

    // Build the AggregateRel with the new grouping_expressions field
    AggregateRel aggrProto =
        AggregateRel.newBuilder()
            .setInput(io.substrait.proto.Rel.newBuilder().setRead(readProto))
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


    //    Relation to Proto where both deprecated and new form are implemented
      RelProtoConverter relToProtoConverter = new RelProtoConverter(functionCollector);
      Rel newProto = relToProtoConverter.toProto(resultRel);

      assertEquals(2, newProto.getAggregate().getGroupings(0).getExpressionReferencesCount());
      assertEquals(2, newProto.getAggregate().getGroupings(0).getGroupingExpressionsList().size());
      assertEquals(2, newProto.getAggregate().getGroupingExpressionsList().size());
  }
}
