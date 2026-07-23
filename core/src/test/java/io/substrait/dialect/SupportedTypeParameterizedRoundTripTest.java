package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.Error;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the parameterized-type constraints {@code max_length} (for {@code FIXED_BINARY},
 * {@code VARCHAR}, {@code FIXED_CHAR}) and {@code max_precision}/{@code max_scale} (for {@code
 * DECIMAL}) serialize as configuration objects, validate against the published {@code
 * dialect_schema.yaml}, and survive a POJO &rarr; YAML &rarr; POJO round trip.
 */
class SupportedTypeParameterizedRoundTripTest {

  private static Dialect dialectWith(SupportedType type) {
    return Dialect.builder().addSupportedTypes(type).build();
  }

  private static void assertRoundTrips(SupportedType type) {
    assertFalse(type.isBare(), "configured type must not be bare");
    Dialect original = dialectWith(type);
    String yaml = Dialect.toYaml(original);

    List<Error> errors = SchemaValidator.validate(yaml);
    assertTrue(errors.isEmpty(), () -> "Generated dialect failed schema validation: " + errors);

    assertEquals(original, Dialect.load(yaml));
  }

  @Test
  void fixedBinaryMaxLength() {
    SupportedType type =
        SupportedType.builder().type(TypeKind.FIXED_BINARY).maxLength(1024).build();

    assertTrue(Dialect.toYaml(dialectWith(type)).contains("max_length: 1024"));
    assertRoundTrips(type);
  }

  @Test
  void varcharMaxLength() {
    SupportedType type = SupportedType.builder().type(TypeKind.VARCHAR).maxLength(255).build();

    assertTrue(Dialect.toYaml(dialectWith(type)).contains("max_length: 255"));
    assertRoundTrips(type);
  }

  @Test
  void fixedCharMaxLength() {
    SupportedType type = SupportedType.builder().type(TypeKind.FIXED_CHAR).maxLength(64).build();

    assertTrue(Dialect.toYaml(dialectWith(type)).contains("max_length: 64"));
    assertRoundTrips(type);
  }

  @Test
  void decimalMaxPrecisionAndScale() {
    SupportedType type =
        SupportedType.builder().type(TypeKind.DECIMAL).maxPrecision(38).maxScale(10).build();

    String yaml = Dialect.toYaml(dialectWith(type));
    assertTrue(yaml.contains("max_precision: 38"), yaml);
    assertTrue(yaml.contains("max_scale: 10"), yaml);
    assertRoundTrips(type);
  }
}
