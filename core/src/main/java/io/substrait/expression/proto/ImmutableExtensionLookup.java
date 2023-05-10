package io.substrait.expression.proto;

import io.substrait.function.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a mapping between function anchors and function references. Generates references for
 * new anchors.
 */
public class ImmutableExtensionLookup extends AbstractExtensionLookup {
  // TODO: Move to io.substrait.extension
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ImmutableExtensionLookup.class);

  private int counter = -1;

  private ImmutableExtensionLookup(
      Map<Integer, SimpleExtension.FunctionAnchor> functionMap,
      Map<Integer, SimpleExtension.TypeAnchor> typeMap) {
    super(functionMap, typeMap);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<Integer, SimpleExtension.FunctionAnchor> functionMap = new HashMap<>();
    private final Map<Integer, SimpleExtension.TypeAnchor> typeMap = new HashMap<>();

    public Builder from(Plan p) {
      Map<Integer, String> namespaceMap = new HashMap<>();
      for (var extension : p.getExtensionUrisList()) {
        namespaceMap.put(extension.getExtensionUriAnchor(), extension.getUri());
      }

      // Add all functions used in plan to the functionMap
      for (var extension : p.getExtensionsList()) {
        if (!extension.hasExtensionFunction()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionFunction func = extension.getExtensionFunction();
        int reference = func.getFunctionAnchor();
        String namespace = namespaceMap.get(func.getExtensionUriReference());
        if (namespace == null) {
          throw new IllegalStateException(
              "Could not find extension URI of " + func.getExtensionUriReference());
        }
        String name = func.getName();
        SimpleExtension.FunctionAnchor anchor = SimpleExtension.FunctionAnchor.of(namespace, name);
        functionMap.put(reference, anchor);
      }

      // Add all types used in plan to the typeMap
      for (var extension : p.getExtensionsList()) {
        if (!extension.hasExtensionType()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionType type = extension.getExtensionType();
        int reference = type.getTypeAnchor();
        String namespace = namespaceMap.get(type.getExtensionUriReference());
        if (namespace == null) {
          throw new IllegalStateException(
              "Could not find extension URI of " + type.getExtensionUriReference());
        }
        String name = type.getName();
        SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(namespace, name);
        typeMap.put(reference, anchor);
      }

      return this;
    }

    public ImmutableExtensionLookup build() {
      return new ImmutableExtensionLookup(
          Collections.unmodifiableMap(functionMap), Collections.unmodifiableMap(typeMap));
    }
  }
}
