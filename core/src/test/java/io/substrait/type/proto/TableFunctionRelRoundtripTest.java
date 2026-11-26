package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.Rel;
import io.substrait.relation.TableFunction;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TableFunctionRelRoundtripTest extends TestBase {

  private static final String GENERATE_SERIES_I64 = "generate_series:i64_i64_i64";

  private TableFunction generateSeries(Expression start, Expression stop, Expression step) {
    return sb.tableFunction(
        DefaultExtensionCatalog.FUNCTIONS_TABLE,
        GENERATE_SERIES_I64,
        R.struct(R.I64),
        start,
        stop,
        step);
  }

  private io.substrait.proto.Rel withFunction(
      io.substrait.proto.Rel rel, io.substrait.proto.TableFunction function) {
    return rel.toBuilder()
        .setTableFunction(rel.getTableFunction().toBuilder().setFunction(function))
        .build();
  }

  @Test
  void generateSeries() {
    TableFunction series = generateSeries(sb.i64(1), sb.i64(100), sb.i64(1));

    assertEquals(R.struct(R.I64), series.getRecordType());
    verifyRoundTrip(series);
  }

  @Test
  void generateSeriesI32() {
    verifyRoundTrip(
        sb.tableFunction(
            DefaultExtensionCatalog.FUNCTIONS_TABLE,
            "generate_series:i32_i32_i32",
            R.struct(R.I32),
            sb.i32(10),
            sb.i32(1),
            sb.i32(-1)));
  }

  @Test
  void argumentsReferenceTheLateralJoinLeftRow() {
    Rel bounds =
        sb.namedScan(
            Collections.singletonList("series_bounds"),
            Collections.singletonList("n"),
            Collections.singletonList(R.I64));
    int anchor = 7;
    TableFunction series =
        generateSeries(
            sb.i64(1),
            FieldReference.newRootStructOuterReferenceByRelReference(
                0, bounds.getRecordType(), anchor),
            sb.i64(1));
    LateralJoin join =
        LateralJoin.builder()
            .left(bounds)
            .right(series)
            .condition(sb.bool(true))
            .joinType(Join.JoinType.INNER)
            .relAnchor(anchor)
            .build();

    assertEquals(R.struct(R.I64, R.I64), join.getRecordType());
    verifyRoundTrip(join);
  }

  @Test
  void nullableSchemaIsRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                sb.tableFunction(
                    DefaultExtensionCatalog.FUNCTIONS_TABLE,
                    GENERATE_SERIES_I64,
                    N.struct(R.I64),
                    sb.i64(1),
                    sb.i64(10),
                    sb.i64(1)));
    assertTrue(e.getMessage().contains("non-nullable schema"), e.getMessage());
  }

  @Test
  void protoWithoutSchemaIsRejected() {
    io.substrait.proto.Rel proto =
        relProtoConverter.toProto(generateSeries(sb.i64(1), sb.i64(10), sb.i64(1)));
    io.substrait.proto.Rel withoutSchema =
        withFunction(
            proto, proto.getTableFunction().getFunction().toBuilder().clearSchema().build());

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> protoRelConverter.from(withoutSchema));
    assertTrue(e.getMessage().contains("schema"), e.getMessage());
  }

  @Test
  void protoReferencingAScalarFunctionIsRejected() {
    io.substrait.proto.Rel proto =
        relProtoConverter.toProto(generateSeries(sb.i64(1), sb.i64(10), sb.i64(1)));
    int scalarReference =
        functionCollector.getFunctionReference(
            extensions.getScalarFunction(
                SimpleExtension.FunctionAnchor.of(
                    DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i64_i64")));
    io.substrait.proto.Rel referencingScalar =
        withFunction(
            proto,
            proto.getTableFunction().getFunction().toBuilder()
                .setFunctionReference(scalarReference)
                .build());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> protoRelConverter.from(referencingScalar));
    assertTrue(e.getMessage().contains("add:i64_i64"), e.getMessage());
  }
}
