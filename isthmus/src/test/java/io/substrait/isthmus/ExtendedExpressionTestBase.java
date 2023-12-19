package io.substrait.isthmus;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;
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
    return assertProtoExtendedExpressionRoundrip(query, new SqlExpressionToSubstrait());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundrip(
      String query, SqlExpressionToSubstrait s) throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundrip(query, s, tpchSchemaCreateStatements());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundrip(
      String query, SqlExpressionToSubstrait s, List<String> creates)
      throws SqlParseException, IOException {
    // proto initial extended expression
    ExtendedExpression extendedExpressionProtoInitial = s.convert(query, creates);

    // pojo final extended expression
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProtoInitial);

    // proto final extended expression
    ExtendedExpression extendedExpressionProtoFinal =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoFinal);

    // round-trip to validate extended expression proto initial equals to final
    Assertions.assertEquals(extendedExpressionProtoFinal, extendedExpressionProtoInitial);

    return extendedExpressionProtoInitial;
  }
}
