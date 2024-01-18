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

  public static List<String> tpchSchemaCreateStatements(String schemaToLoad) throws IOException {
    String[] values = asString(schemaToLoad).split(";");
    return Arrays.stream(values)
        .filter(t -> !t.trim().isBlank())
        .collect(java.util.stream.Collectors.toList());
  }

  public static List<String> tpchSchemaCreateStatements() throws IOException {
    return tpchSchemaCreateStatements("tpch/schema.sql");
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundtrip(String query)
      throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundtrip(query, new SqlExpressionToSubstrait());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundtrip(
      String query, String schemaToLoad) throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundtrip(
        query, new SqlExpressionToSubstrait(), schemaToLoad);
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundtrip(
      String query, SqlExpressionToSubstrait s) throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundtrip(query, s, tpchSchemaCreateStatements());
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundtrip(
      String query, SqlExpressionToSubstrait s, String schemaToLoad)
      throws IOException, SqlParseException {
    return assertProtoExtendedExpressionRoundtrip(
        query, s, tpchSchemaCreateStatements(schemaToLoad));
  }

  protected ExtendedExpression assertProtoExtendedExpressionRoundtrip(
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
