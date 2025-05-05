package io.substrait.consumercatalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Enclosing
public abstract class ConsumerCatalog {

  public static Catalog load(InputStream catalog) {
    try {
      return objectMapper().readValue(catalog, Catalog.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Catalog load(String catalog) {
    try {
      return objectMapper().readValue(catalog, Catalog.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static ObjectMapper objectMapper() {
    return new ObjectMapper(new YAMLFactory())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .registerModule(new Jdk8Module());
  }

  private ConsumerCatalog() {}

  @JsonDeserialize(as = ImmutableConsumerCatalog.Catalog.class)
  @Value.Immutable
  public abstract static class Catalog {
    public abstract List<ExtensionMapping> mappings();
  }

  @JsonDeserialize(as = ImmutableConsumerCatalog.ExtensionMapping.class)
  @Value.Immutable
  public abstract static class ExtensionMapping {
    public abstract String uri();

    @JsonProperty("scalar_functions")
    public abstract List<ScalarFunctionMapping> scalarFunctionMappings();
  }

  @JsonDeserialize(as = ImmutableConsumerCatalog.ScalarFunctionMapping.class)
  @Value.Immutable
  public abstract static class ScalarFunctionMapping {
    public abstract String name();

    public abstract List<FunctionVariant> variants();

    public abstract Map<String, String> meta();
  }

  @JsonDeserialize(as = ImmutableConsumerCatalog.FunctionVariant.class)
  @Value.Immutable
  public abstract static class FunctionVariant {
    public abstract String signature();
  }
}
