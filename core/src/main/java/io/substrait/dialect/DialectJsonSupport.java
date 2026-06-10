package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Shared helpers for the dialect union (de)serializers. */
final class DialectJsonSupport {

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  private DialectJsonSupport() {}

  /** Read the {@code metadata} object on a node into a map, or {@code null} when absent. */
  static Map<String, Object> readMetadata(JsonParser p, JsonNode node) {
    JsonNode metadata = node.get("metadata");
    if (metadata == null || metadata.isNull()) {
      return null;
    }
    return ((ObjectMapper) p.getCodec()).convertValue(metadata, MAP_TYPE);
  }

  /**
   * Read a YAML/JSON array node into a list of enum constants. Missing nodes yield an empty list.
   */
  static <E extends Enum<E>> List<E> readEnums(JsonNode node, Class<E> type) {
    List<E> result = new ArrayList<>();
    if (node != null && node.isArray()) {
      for (JsonNode element : node) {
        result.add(Enum.valueOf(type, element.asText()));
      }
    }
    return result;
  }

  /** Read a node into a list of strings. Missing nodes yield an empty list. */
  static List<String> readStrings(JsonNode node) {
    List<String> result = new ArrayList<>();
    if (node != null && node.isArray()) {
      for (JsonNode element : node) {
        result.add(element.asText());
      }
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
