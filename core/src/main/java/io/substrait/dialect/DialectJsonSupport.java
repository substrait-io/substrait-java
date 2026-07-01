package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Shared helpers and the single, shared {@link ObjectMapper} for the dialect (de)serializers. */
final class DialectJsonSupport {

  /**
   * A single, shared mapper. {@link ObjectMapper} is thread-safe once configured, so the dialect
   * code reuses one instance for all (de)serialization. The custom union (de)serializers also read
   * it directly rather than casting {@code JsonParser#getCodec()}.
   */
  static final ObjectMapper MAPPER =
      new ObjectMapper(new YAMLFactory())
          .registerModule(new Jdk8Module())
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          // Omit absent Optionals and empty collections so that unset sections are not emitted.
          // The custom (de)serializers for the polymorphic unions write their fields explicitly and
          // are unaffected by this inclusion setting.
          .setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  private DialectJsonSupport() {}

  /** Read the {@code metadata} object on a node into a map, or {@code null} when absent. */
  static Map<String, Object> readMetadata(JsonNode node) {
    JsonNode metadata = node.get("metadata");
    if (metadata == null || metadata.isNull()) {
      return null;
    }
    return MAPPER.convertValue(metadata, MAP_TYPE);
  }

  /**
   * Read a YAML/JSON node into a list of enum constants. Both array nodes and a single scalar node
   * (consistent with {@code ACCEPT_SINGLE_VALUE_AS_ARRAY}) are accepted. Missing nodes yield an
   * empty list.
   */
  static <E extends Enum<E>> List<E> readEnums(JsonNode node, Class<E> type) {
    List<E> result = new ArrayList<>();
    if (node == null || node.isNull()) {
      return result;
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        result.add(Enum.valueOf(type, element.asText()));
      }
    } else {
      result.add(Enum.valueOf(type, node.asText()));
    }
    return result;
  }

  /**
   * Read a node into a list of strings. Both array nodes and a single scalar node are accepted.
   * Missing nodes yield an empty list.
   */
  static List<String> readStrings(JsonNode node) {
    List<String> result = new ArrayList<>();
    if (node == null || node.isNull()) {
      return result;
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        result.add(element.asText());
      }
    } else {
      result.add(node.asText());
    }
    return result;
  }

  /** Write a list of enum constants as a JSON array field, using each constant's name. */
  static void writeEnumArray(JsonGenerator gen, String fieldName, List<? extends Enum<?>> values)
      throws IOException {
    gen.writeArrayFieldStart(fieldName);
    for (Enum<?> value : values) {
      gen.writeString(value.name());
    }
    gen.writeEndArray();
  }

  /** Write a list of strings as a JSON array field. */
  static void writeStringArray(JsonGenerator gen, String fieldName, List<String> values)
      throws IOException {
    gen.writeArrayFieldStart(fieldName);
    for (String value : values) {
      gen.writeString(value);
    }
    gen.writeEndArray();
  }

  /** Write an optional metadata map as a {@code metadata} field if present. */
  static void writeMetadata(
      JsonGenerator gen, SerializerProvider provider, Map<String, Object> metadata)
      throws IOException {
    provider.defaultSerializeField("metadata", metadata, gen);
  }
}
