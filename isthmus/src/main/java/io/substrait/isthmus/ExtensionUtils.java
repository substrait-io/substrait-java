package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class ExtensionUtils {

  public static SimpleExtension.ExtensionCollection getDynamicExtensions(
      SimpleExtension.ExtensionCollection extensions) {
    Set<String> knownFunctionNames =
        SubstraitOperatorTable.INSTANCE.getOperatorList().stream()
            .map(op -> op.getName().toLowerCase(Locale.ROOT))
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

  public static SimpleExtension.ExtensionCollection loadExtensions(List<String> yamlFunctionFiles) {
    SimpleExtension.ExtensionCollection allExtensions = SimpleExtension.loadDefaults();
    if (yamlFunctionFiles != null && !yamlFunctionFiles.isEmpty()) {
      allExtensions = allExtensions.merge(SimpleExtension.load(yamlFunctionFiles));
    }
    return allExtensions;
  }
}
