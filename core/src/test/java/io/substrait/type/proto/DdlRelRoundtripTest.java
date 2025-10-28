package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.ExtensionDdl;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class DdlRelRoundtripTest extends TestBase {

  @Test
  void create() {
    NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    Expression.StructNested defaults =
        Expression.StructNested.builder()
            .addFields(ExpressionCreator.i64(false, 1))
            .addFields(ExpressionCreator.i64(false, 2))
            .build();

    NamedDdl command =
        NamedDdl.builder()
            .tableSchema(schema)
            .tableDefaults(defaults)
            .names(Stream.of("table").collect(Collectors.toList()))
            .operation(NamedDdl.DdlOp.CREATE)
            .object(NamedDdl.DdlObject.TABLE)
            .build();

    verifyRoundTrip(command);
  }

  @Test
  void alter() {
    ProtoRelConverter protoRelConverter =
        new StringHolderHandlingProtoRelConverter(functionCollector, defaultExtensionCollection);

    StringHolder detail = new StringHolder("DETAIL");

    NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    Expression.StructNested defaults =
        Expression.StructNested.builder()
            .addFields(ExpressionCreator.i64(false, 1))
            .addFields(ExpressionCreator.i64(false, 2))
            .build();

    VirtualTableScan virtTable =
        VirtualTableScan.builder().initialSchema(schema).addRows(defaults).build();

    ExtensionDdl command =
        ExtensionDdl.builder()
            .viewDefinition(virtTable)
            .tableSchema(schema)
            .tableDefaults(defaults)
            .detail(detail)
            .operation(ExtensionDdl.DdlOp.ALTER)
            .object(ExtensionDdl.DdlObject.VIEW)
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(command);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(command, relReturned);
  }
}
