package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.ExtensionWrite;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class WriteRelRoundtripTest extends TestBase {

  @Test
  void insert() {
    NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    VirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                ExpressionCreator.struct(
                    false, ExpressionCreator.i64(false, 1), ExpressionCreator.i64(false, 2)))
            .build();
    virtTable =
        VirtualTableScan.builder()
            .from(virtTable)
            .filter(b.equal(b.fieldReference(virtTable, 0), b.fieldReference(virtTable, 1)))
            .build();

    NamedWrite command =
        NamedWrite.builder()
            .input(virtTable)
            .tableSchema(schema)
            .operation(NamedWrite.WriteOp.INSERT)
            .names(Stream.of("table").collect(Collectors.toList()))
            .createMode(NamedWrite.CreateMode.REPLACE_IF_EXISTS)
            .outputMode(NamedWrite.OutputMode.NO_OUTPUT)
            .build();

    verifyRoundTrip(command);
  }

  @Test
  void append() {
    ProtoRelConverter protoRelConverter =
        new StringHolderHandlingProtoRelConverter(functionCollector, defaultExtensionCollection);

    StringHolder detail = new StringHolder("DETAIL");

    NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    VirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                ExpressionCreator.struct(
                    false, ExpressionCreator.i64(false, 1), ExpressionCreator.i64(false, 2)))
            .build();

    ExtensionWrite command =
        ExtensionWrite.builder()
            .input(virtTable)
            .tableSchema(schema)
            .operation(ExtensionWrite.WriteOp.INSERT)
            .detail(detail)
            .createMode(ExtensionWrite.CreateMode.APPEND_IF_EXISTS)
            .outputMode(ExtensionWrite.OutputMode.NO_OUTPUT)
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(command);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(command, relReturned);
  }
}
