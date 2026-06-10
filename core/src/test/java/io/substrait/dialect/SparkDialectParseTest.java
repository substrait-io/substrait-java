package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.Error;
import io.substrait.dialect.Dialect.DialectDocument;
import io.substrait.dialect.Dialect.ExpressionKind;
import io.substrait.dialect.Dialect.RelationKind;
import io.substrait.dialect.Dialect.SubqueryType;
import io.substrait.dialect.Dialect.SupportedExpression;
import io.substrait.dialect.Dialect.SupportedRelation;
import io.substrait.dialect.Dialect.SupportedType;
import io.substrait.dialect.Dialect.TypeKind;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Parses the real-world {@code spark_dialect.yaml} (copied onto the test classpath by the build) to
 * confirm the model consumes a production dialect, and re-validates it against the schema.
 */
class SparkDialectParseTest {

  private static final DialectDocument SPARK = Dialect.loadResource("/dialect/spark_dialect.yaml");

  @Test
  void parsesTopLevelFields() {
    assertEquals("Spark Dialect", SPARK.name().orElse(null));
    assertTrue(SPARK.dependencies().containsKey("spark"));
    assertTrue(SPARK.supportedScalarFunctions().size() > 0);
  }

  @Test
  void parsesConfiguredRelationsAndExpressions() {
    SupportedRelation join =
        SPARK.supportedRelations().stream()
            .filter(r -> r.relation() == RelationKind.JOIN)
            .findFirst()
            .orElseThrow();
    assertTrue(join.joinTypes().contains(Dialect.JoinType.INNER));

    SupportedExpression subquery =
        SPARK.supportedExpressions().stream()
            .filter(e -> e.expression() == ExpressionKind.SUBQUERY)
            .findFirst()
            .orElseThrow();
    assertEquals(List.of(SubqueryType.SCALAR, SubqueryType.IN_PREDICATE), subquery.subqueryTypes());
  }

  @Test
  void parsesPrecisionTypes() {
    SupportedType precisionTimestamp =
        SPARK.supportedTypes().stream()
            .filter(t -> t.type() == TypeKind.PRECISION_TIMESTAMP)
            .findFirst()
            .orElseThrow();
    assertEquals(9, precisionTimestamp.maxPrecision().orElse(-1));
  }

  @Test
  void reserializesToSchemaValidYaml() {
    String yaml = Dialect.toYaml(SPARK);
    List<Error> errors = SchemaValidator.validate(yaml);
    assertTrue(errors.isEmpty(), () -> "Re-serialized Spark dialect failed validation: " + errors);
  }
}
