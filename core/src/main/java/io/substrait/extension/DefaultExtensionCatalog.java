package io.substrait.extension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultExtensionCatalog {
  public static final String FUNCTIONS_AGGREGATE_APPROX =
      "extension:io.substrait:functions_aggregate_approx";
  public static final String FUNCTIONS_AGGREGATE_GENERIC =
      "extension:io.substrait:functions_aggregate_generic";
  public static final String FUNCTIONS_ARITHMETIC = "extension:io.substrait:functions_arithmetic";
  public static final String FUNCTIONS_ARITHMETIC_DECIMAL =
      "extension:io.substrait:functions_arithmetic_decimal";
  public static final String FUNCTIONS_BOOLEAN = "extension:io.substrait:functions_boolean";
  public static final String FUNCTIONS_COMPARISON = "extension:io.substrait:functions_comparison";
  public static final String FUNCTIONS_DATETIME = "extension:io.substrait:functions_datetime";
  public static final String FUNCTIONS_GEOMETRY = "extension:io.substrait:functions_geometry";
  public static final String FUNCTIONS_LOGARITHMIC = "extension:io.substrait:functions_logarithmic";
  public static final String FUNCTIONS_ROUNDING = "extension:io.substrait:functions_rounding";
  public static final String FUNCTIONS_ROUNDING_DECIMAL =
      "extension:io.substrait:functions_rounding_decimal";
  public static final String FUNCTIONS_SET = "extension:io.substrait:functions_set";
  public static final String FUNCTIONS_STRING = "extension:io.substrait:functions_string";

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
