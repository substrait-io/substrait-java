package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortDirection;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression.BoolLiteral;
import io.substrait.expression.ImmutableExpression.SortField;
import io.substrait.expression.ImmutableExpression.StructLiteral;
import io.substrait.relation.AbstractDdlRel.DdlObject;
import io.substrait.relation.AbstractDdlRel.DdlOp;
import io.substrait.relation.AbstractUpdate.TransformExpression;
import io.substrait.relation.AbstractWriteRel.CreateMode;
import io.substrait.relation.AbstractWriteRel.OutputMode;
import io.substrait.relation.AbstractWriteRel.WriteOp;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.ExtensionDdl;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.ExtensionWrite;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.Set;
import io.substrait.relation.Set.SetOp;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingExtensionProtoConverter;
import io.substrait.utils.StringHolderHandlingProtoExtensionConverter;
import org.junit.jupiter.api.Test;

class AdvancedExtensionRelProtoConversionTest extends TestBase {
  final StringHolder enhanced = new StringHolder("ENHANCED");
  final StringHolder optimized = new StringHolder("OPTIMIZED");
  final AdvancedExtension<?, ?> extension =
      AdvancedExtension.builder().enhancement(enhanced).addOptimizations(optimized).build();

  private void assertRoundTrip(final Rel rel) {
    // assert AdvancedExtension serialization to proto
    final ExtensionCollector functionCollector = new ExtensionCollector();
    final RelProtoConverter relProtoConverter =
        new RelProtoConverter(functionCollector, new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Rel protoRel =
        rel.accept(relProtoConverter, EmptyVisitationContext.INSTANCE);

    // assert AdvancedExtension deserialization from proto
    final ProtoRelConverter protoRelConverter =
        new ProtoRelConverter(functionCollector, new StringHolderHandlingProtoExtensionConverter());
    final Rel rel2 = protoRelConverter.from(protoRel);

    assertEquals(rel, rel2);
  }

  @Test
  void testVirtualTableConversionRoundtrip() throws Exception {
    final VirtualTableScan rel =
        VirtualTableScan.builder()
            .extension(extension)
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(BoolLiteral.builder().value(true).build())
                    .build())
            .initialSchema(
                NamedStruct.builder()
                    .addNames("IS_TRUE")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testLocalFilesConversionRoundtrip() throws Exception {
    final LocalFiles rel =
        LocalFiles.builder()
            .initialSchema(NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testNamedScanConversionRoundtrip() throws Exception {
    final NamedScan rel =
        NamedScan.builder()
            .addNames("CUSTOMER")
            .initialSchema(NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testExtensionTableConversionRoundtrip() throws Exception {
    final ExtensionTable rel =
        ExtensionTable.builder()
            .detail(new EmptyDetail())
            .initialSchema(NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testFilterRelConversionRoundtrip() throws Exception {
    final Filter rel =
        Filter.builder()
            .input(
                EmptyScan.builder()
                    .initialSchema(
                        NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
                    .build())
            .condition(ExpressionCreator.bool(false, true))
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testFetchRelConversionRoundtrip() throws Exception {
    final Fetch rel =
        Fetch.builder()
            .input(
                EmptyScan.builder()
                    .initialSchema(
                        NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
                    .build())
            .offset(0)
            .count(10)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testAggregateRelConversionRoundtrip() throws Exception {
    final Aggregate rel =
        Aggregate.builder()
            .input(
                EmptyScan.builder()
                    .initialSchema(
                        NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
                    .build())
            .addMeasures(sb.countStar())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testSortRelConversionRoundtrip() throws Exception {
    final EmptyScan scan =
        EmptyScan.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("KEY")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();
    final Sort rel =
        Sort.builder()
            .input(scan)
            .addSortFields(
                SortField.builder()
                    .direction(SortDirection.ASC_NULLS_FIRST)
                    .expr(sb.fieldReference(scan, 0))
                    .build())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testJoinRelConversionRoundtrip() throws Exception {
    final EmptyScan scan =
        EmptyScan.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("KEY")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();
    final Join rel =
        Join.builder()
            .left(scan)
            .right(scan)
            .joinType(JoinType.INNER)
            .condition(sb.equal(sb.fieldReference(scan, 0), sb.fieldReference(scan, 0)))
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testProjectRelConversionRoundtrip() throws Exception {
    final EmptyScan scan =
        EmptyScan.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("KEY")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();
    final Project rel =
        Project.builder()
            .input(scan)
            .addExpressions(sb.fieldReference(scan, 0))
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testSetRelConversionRoundtrip() throws Exception {
    final EmptyScan scan =
        EmptyScan.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("KEY")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();
    final Set rel =
        Set.builder()
            .addInputs(scan, scan)
            .setOp(SetOp.UNION_DISTINCT)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testCrossRelConversionRoundtrip() throws Exception {
    final EmptyScan scan =
        EmptyScan.builder()
            .initialSchema(
                NamedStruct.builder()
                    .addNames("KEY")
                    .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
                    .build())
            .build();
    final Cross rel = Cross.builder().left(scan).right(scan).extension(extension).build();

    assertRoundTrip(rel);
  }

  @Test
  void testNamedObjectWriteConversionRoundtrip() throws Exception {
    final NamedStruct schema =
        NamedStruct.builder()
            .addNames("KEY")
            .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
            .build();
    final EmptyScan scan = EmptyScan.builder().initialSchema(schema).build();
    final NamedWrite rel =
        NamedWrite.builder()
            .createMode(CreateMode.REPLACE_IF_EXISTS)
            .operation(WriteOp.INSERT)
            .outputMode(OutputMode.NO_OUTPUT)
            .tableSchema(schema)
            .input(scan)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testExtensionWriteRelConversionRoundtrip() throws Exception {
    final NamedStruct schema =
        NamedStruct.builder()
            .addNames("KEY")
            .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
            .build();
    final EmptyScan scan = EmptyScan.builder().initialSchema(schema).build();
    final ExtensionWrite rel =
        ExtensionWrite.builder()
            .createMode(CreateMode.REPLACE_IF_EXISTS)
            .operation(WriteOp.INSERT)
            .outputMode(OutputMode.NO_OUTPUT)
            .tableSchema(schema)
            .detail(new EmptyDetail())
            .input(scan)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testNamedDdlRelConversionRoundtrip() throws Exception {
    final NamedStruct schema =
        NamedStruct.builder()
            .addNames("KEY")
            .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
            .build();
    final EmptyScan scan = EmptyScan.builder().initialSchema(schema).build();
    final NamedDdl rel =
        NamedDdl.builder()
            .addNames("CUSTOMER")
            .operation(DdlOp.CREATE)
            .tableSchema(schema)
            .viewDefinition(scan)
            .tableDefaults(
                StructLiteral.builder()
                    .nullable(false)
                    .addFields(ExpressionCreator.bool(false, false))
                    .build())
            .object(DdlObject.VIEW)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testExtensionDdlRelConversionRoundtrip() throws Exception {
    final NamedStruct schema =
        NamedStruct.builder()
            .addNames("KEY")
            .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
            .build();
    final EmptyScan scan = EmptyScan.builder().initialSchema(schema).build();
    final ExtensionDdl rel =
        ExtensionDdl.builder()
            .detail(new EmptyDetail())
            .operation(DdlOp.CREATE)
            .tableSchema(schema)
            .viewDefinition(scan)
            .tableDefaults(
                StructLiteral.builder()
                    .nullable(false)
                    .addFields(ExpressionCreator.bool(false, false))
                    .build())
            .object(DdlObject.VIEW)
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }

  @Test
  void testNamedUpdateRelConversionRoundtrip() throws Exception {
    final NamedStruct schema =
        NamedStruct.builder()
            .addNames("KEY")
            .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
            .build();
    final NamedUpdate rel =
        NamedUpdate.builder()
            .addNames("CUSTOMER")
            .tableSchema(schema)
            .condition(
                sb.equal(
                    FieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(TypeCreator.REQUIRED.BOOLEAN)
                        .build(),
                    sb.bool(true)))
            .addTransformations(
                TransformExpression.builder()
                    .columnTarget(0)
                    .transformation(ExpressionCreator.bool(false, false))
                    .build())
            .extension(extension)
            .build();

    assertRoundTrip(rel);
  }
}
