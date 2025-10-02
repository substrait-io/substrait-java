package io.substrait.extension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultExtensionCatalog {
  public static final String FUNCTIONS_AGGREGATE_APPROX = "/functions_aggregate_approx.yaml";
  public static final String FUNCTIONS_AGGREGATE_GENERIC = "/functions_aggregate_generic.yaml";
  public static final String FUNCTIONS_ARITHMETIC = "/functions_arithmetic.yaml";
  public static final String FUNCTIONS_ARITHMETIC_DECIMAL = "/functions_arithmetic_decimal.yaml";
  public static final String FUNCTIONS_BOOLEAN = "/functions_boolean.yaml";
  public static final String FUNCTIONS_COMPARISON = "/functions_comparison.yaml";
  public static final String FUNCTIONS_DATETIME = "/functions_datetime.yaml";
  public static final String FUNCTIONS_GEOMETRY = "/functions_geometry.yaml";
  public static final String FUNCTIONS_LOGARITHMIC = "/functions_logarithmic.yaml";
  public static final String FUNCTIONS_ROUNDING = "/functions_rounding.yaml";
  public static final String FUNCTIONS_ROUNDING_DECIMAL = "/functions_rounding_decimal.yaml";
  public static final String FUNCTIONS_SET = "/functions_set.yaml";
  public static final String FUNCTIONS_STRING = "/functions_string.yaml";

  public static final SimpleExtension.ExtensionCollection DEFAULT_COLLECTION =
      loadDefaultCollection();

  private static SimpleExtension.ExtensionCollection loadDefaultCollection() {
    List<String> defaultFiles =
        Arrays.asList(
                "boolean",
                "aggregate_generic",
                "aggregate_approx",
                "arithmetic_decimal",
                "arithmetic",
                "comparison",
                "datetime",
                "logarithmic",
                "rounding",
                "rounding_decimal",
                "string")
            .stream()
            .map(c -> String.format("/functions_%s.yaml", c))
            .collect(Collectors.toList());

    return SimpleExtension.load(defaultFiles);
  }
}
