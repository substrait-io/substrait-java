package io.substrait.isthmus;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.extended.expression.ExtendedExpressionProtoConverter;
import io.substrait.extended.expression.ProtoExtendedExpressionConverter;
import io.substrait.proto.ExtendedExpression;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;

public class ExtendedExpressionTestBase {
  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  public static List<String> tpchSchemaCreateStatements() throws IOException {
    String[] values = asString("tpch/schema.sql").split(";");
    return Arrays.stream(values)
        .filter(t -> !t.trim().isBlank())
        .collect(java.util.stream.Collectors.toList());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundrip(String query)
      throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundrip(query, new SqlToSubstrait());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundrip(String query, SqlToSubstrait s)
      throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundrip(query, s, tpchSchemaCreateStatements());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundrip(
      String query, SqlToSubstrait s, List<String> creates) throws SqlParseException, IOException {
    io.substrait.proto.ExtendedExpression protoExtendedExpression =
        s.executeSQLExpression(query, creates);

    try {
      String ee = JsonFormat.printer().print(protoExtendedExpression);
      System.out.println("Proto Extended Expression: \n" + ee);

      io.substrait.extended.expression.ExtendedExpression from =
          new ProtoExtendedExpressionConverter().from(protoExtendedExpression);

      ExtendedExpression proto = new ExtendedExpressionProtoConverter().toProto(from);

      Assertions.assertEquals(proto, protoExtendedExpression);
      // FIXME! Implement test validation as the same as proto Plan implementation
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    return protoExtendedExpression;
  }
}
