package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.FunctionMappings;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class ExtensionUtils {

  /**
   * Extracts dynamic extensions from a collection of extensions.
   *
   * <p>A <b>dynamic extension</b> is a user-defined function (UDF) that is not part of the standard
   * Substrait function catalog. These are custom functions that users define and provide at
   * runtime, extending the built-in function set with domain-specific or application-specific
   * operations.
   *
   * <p>This method filters out all functions that are already known to the Calcite operator table
   * (the standard/built-in functions) and returns only the custom functions that represent new
   * capabilities not available in the default function set.
   *
   * <p><b>Example:</b> If a user defines a custom UDF "my_hash_function" that computes a
   * proprietary hash, this would be a dynamic extension since it's not part of the standard
   * Substrait specification.
   *
   * @param extensions the complete collection of extensions (both standard and custom)
   * @return a new ExtensionCollection containing only the dynamic (custom/user-defined) functions
   *     that are not present in the standard Substrait function catalog
   */
  public static SimpleExtension.ExtensionCollection getDynamicExtensions(
      SimpleExtension.ExtensionCollection extensions) {
    Set<String> knownFunctionNames =
        FunctionMappings.SCALAR_SIGS.stream()
            .map(FunctionMappings.Sig::name)
            .collect(Collectors.toSet());

    List<SimpleExtension.ScalarFunctionVariant> customFunctions =
        extensions.scalarFunctions().stream()
            .filter(f -> !knownFunctionNames.contains(f.name().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());

    return SimpleExtension.ExtensionCollection.builder()
        .scalarFunctions(customFunctions)
        // TODO: handle aggregates and other functions
        .build();
  }
}
