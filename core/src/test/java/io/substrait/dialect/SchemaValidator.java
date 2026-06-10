package io.substrait.dialect;

import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Validates dialect YAML against the published {@code dialect_schema.yaml}, which is placed on the
 * test classpath by the {@code processTestResources} task in {@code core/build.gradle.kts}.
 */
final class SchemaValidator {

  private static final String SCHEMA_RESOURCE = "/substrait/text/dialect_schema.yaml";

  private static final Schema SCHEMA = loadSchema();

  private SchemaValidator() {}

  private static Schema loadSchema() {
    try (InputStream stream = SchemaValidator.class.getResourceAsStream(SCHEMA_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException(
            "Dialect schema not found on classpath: " + SCHEMA_RESOURCE);
      }
      return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
          .getSchema(stream, InputFormat.YAML);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Returns the validation errors produced by checking {@code yaml} against the schema. */
  static List<Error> validate(String yaml) {
    return SCHEMA.validate(yaml, InputFormat.YAML);
  }
}
