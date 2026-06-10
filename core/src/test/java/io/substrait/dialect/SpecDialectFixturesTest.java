package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.Error;
import io.substrait.dialect.Dialect.DialectDocument;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the per-section dialect fixtures published by the substrait spec (copied onto the test
 * classpath by the build). For each fixture we confirm it is schema-valid, parses, and survives a
 * lossless serialize/parse round-trip whose output is itself schema-valid.
 */
class SpecDialectFixturesTest {

  private static String readResource(String resourcePath) {
    try (InputStream stream = SpecDialectFixturesTest.class.getResourceAsStream(resourcePath)) {
      if (stream == null) {
        throw new IllegalStateException("Fixture not found on classpath: " + resourcePath);
      }
      try (Scanner scanner = new Scanner(stream, StandardCharsets.UTF_8.name())) {
        scanner.useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "types_test.yaml",
        "relations_test.yaml",
        "expressions_test.yaml",
        "functions_test.yaml",
        "execution_behavior_test.yaml"
      })
  void roundTrips(String fixture) {
    String resourcePath = "/dialect/tests/" + fixture;
    String original = readResource(resourcePath);

    // The published fixture is itself schema-valid.
    List<Error> fixtureErrors = SchemaValidator.validate(original);
    assertTrue(fixtureErrors.isEmpty(), () -> fixture + " is not schema-valid: " + fixtureErrors);

    // Parse, re-serialize, and confirm the output is still schema-valid.
    DialectDocument parsed = Dialect.loadResource(resourcePath);
    String reserialized = Dialect.toYaml(parsed);
    List<Error> roundTripErrors = SchemaValidator.validate(reserialized);
    assertTrue(
        roundTripErrors.isEmpty(),
        () -> "Re-serialized " + fixture + " is not schema-valid: " + roundTripErrors);

    // Re-parsing the serialized form yields an equal model (lossless round-trip).
    assertEquals(parsed, Dialect.load(reserialized));
  }
}
