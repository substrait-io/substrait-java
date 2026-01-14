package io.substrait.extension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides default extension catalog constants and utilities for loading built-in Substrait
 * function definitions.
 */
public class DefaultExtensionCatalog {

  /** Extension identifier for approximate aggregate functions. */
  public static final String FUNCTIONS_AGGREGATE_APPROX =
      "extension:io.substrait:functions_aggregate_approx";

  /** Extension identifier for generic aggregate functions. */
  public static final String FUNCTIONS_AGGREGATE_GENERIC =
      "extension:io.substrait:functions_aggregate_generic";

  /** Extension identifier for arithmetic functions. */
  public static final String FUNCTIONS_ARITHMETIC = "extension:io.substrait:functions_arithmetic";

  /** Extension identifier for decimal arithmetic functions. */
  public static final String FUNCTIONS_ARITHMETIC_DECIMAL =
      "extension:io.substrait:functions_arithmetic_decimal";

  /** Extension identifier for boolean functions. */
  public static final String FUNCTIONS_BOOLEAN = "extension:io.substrait:functions_boolean";

  /** Extension identifier for comparison functions. */
  public static final String FUNCTIONS_COMPARISON = "extension:io.substrait:functions_comparison";

  /** Extension identifier for datetime functions. */
  public static final String FUNCTIONS_DATETIME = "extension:io.substrait:functions_datetime";

  /** Extension identifier for geometry functions. */
  public static final String FUNCTIONS_GEOMETRY = "extension:io.substrait:functions_geometry";

  /** Extension identifier for logarithmic functions. */
  public static final String FUNCTIONS_LOGARITHMIC = "extension:io.substrait:functions_logarithmic";

  /** Extension identifier for rounding functions. */
  public static final String FUNCTIONS_ROUNDING = "extension:io.substrait:functions_rounding";

  /** Extension identifier for decimal rounding functions. */
  public static final String FUNCTIONS_ROUNDING_DECIMAL =
      "extension:io.substrait:functions_rounding_decimal";

  /** Extension identifier for set functions. */
  public static final String FUNCTIONS_SET = "extension:io.substrait:functions_set";

  /** Extension identifier for string functions. */
  public static final String FUNCTIONS_STRING = "extension:io.substrait:functions_string";
  public static final String EXTENSION_TYPES = "extension:io.substrait:extension_types";

  /** Default collection of built-in extensions loaded from YAML resources. */
  public static final SimpleExtension.ExtensionCollection DEFAULT_COLLECTION =
      loadDefaultCollection();

  /**
   * Loads the default extension collection from predefined YAML files.
   *
   * @return the loaded extension collection
   */
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

    defaultFiles.add("/extension_types.yaml");

    return SimpleExtension.load(defaultFiles);
  }
}
