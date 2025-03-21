package io.substrait.isthmus;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;

public class ExtendedExpressionTestBase {
  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  public static List<String> tpchSchemaCreateStatements(String schemaToLoad) throws IOException {
    String[] values = asString(schemaToLoad).split(";");
    return Arrays.stream(values)
        .filter(t -> !t.trim().isBlank())
        .collect(java.util.stream.Collectors.toList());
  }

  public static List<String> tpchSchemaCreateStatements() throws IOException {
    return tpchSchemaCreateStatements("tpch/schema.sql");
  }

  protected void assertProtoExtendedExpressionRoundtrip(String expressions)
      throws SqlParseException, IOException {
    // proto initial extended expression
    io.substrait.proto.ExtendedExpression extendedExpressionProtoInitial =
        new SqlExpressionToSubstrait().convert(expressions, tpchSchemaCreateStatements());
    asserProtoExtendedExpression(extendedExpressionProtoInitial);
  }

  protected void assertProtoExtendedExpressionRoundtrip(String expressions, String schemaToLoad)
      throws SqlParseException, IOException {
    // proto initial extended expression
    io.substrait.proto.ExtendedExpression extendedExpressionProtoInitial =
        new SqlExpressionToSubstrait()
            .convert(expressions, tpchSchemaCreateStatements(schemaToLoad));
    asserProtoExtendedExpression(extendedExpressionProtoInitial);
  }

  protected void assertProtoExtendedExpressionRoundtrip(String[] expression)
      throws SqlParseException, IOException {
    // proto initial extended expression
    io.substrait.proto.ExtendedExpression extendedExpressionProtoInitial =
        new SqlExpressionToSubstrait().convert(expression, tpchSchemaCreateStatements());
    asserProtoExtendedExpression(extendedExpressionProtoInitial);
  }

  private static void asserProtoExtendedExpression(
      io.substrait.proto.ExtendedExpression extendedExpressionProtoInitial) {
    // pojo final extended expression
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProtoInitial);

    // proto final extended expression
    io.substrait.proto.ExtendedExpression extendedExpressionProtoFinal =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoFinal);

    // round-trip to validate extended expression proto initial equals to final
    Assertions.assertEquals(extendedExpressionProtoFinal, extendedExpressionProtoInitial);
  }
}
