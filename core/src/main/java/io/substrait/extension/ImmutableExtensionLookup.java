package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains a mapping between function anchors and function references. Generates references for
 * new anchors.
 */
public class ImmutableExtensionLookup extends AbstractExtensionLookup {

  private ImmutableExtensionLookup(
      Map<Integer, SimpleExtension.FunctionAnchor> functionMap,
      Map<Integer, SimpleExtension.TypeAnchor> typeMap) {
    super(functionMap, typeMap);
  }

  public static Builder builder() {
    return builder(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public static Builder builder(SimpleExtension.ExtensionCollection extensionCollection) {
    return new Builder(extensionCollection);
  }

  public static class Builder {
    private final Map<Integer, SimpleExtension.FunctionAnchor> functionMap = new HashMap<>();
    private final Map<Integer, SimpleExtension.TypeAnchor> typeMap = new HashMap<>();

    public Builder(SimpleExtension.ExtensionCollection extensionCollection) {
      if (extensionCollection == null) {
        throw new IllegalArgumentException("ExtensionCollection is required");
      }
    }

    private SimpleExtension.FunctionAnchor resolveFunctionAnchor(
        SimpleExtensionDeclaration.ExtensionFunction func, Map<Integer, String> urnMap) {

      String urn = urnMap.get(func.getExtensionUrnReference());
      if (urn == null) {
        throw new IllegalStateException(
            String.format(
                "Function '%s' references URN anchor %d, but no URN is registered at that anchor",
                func.getName(), func.getExtensionUrnReference()));
      }
      return SimpleExtension.FunctionAnchor.of(urn, func.getName());
    }

    private SimpleExtension.TypeAnchor resolveTypeAnchor(
        SimpleExtensionDeclaration.ExtensionType type, Map<Integer, String> urnMap) {

      String urn = urnMap.get(type.getExtensionUrnReference());
      if (urn == null) {
        throw new IllegalStateException(
            String.format(
                "Type '%s' references URN anchor %d, but no URN is registered at that anchor",
                type.getName(), type.getExtensionUrnReference()));
      }
      return SimpleExtension.TypeAnchor.of(urn, type.getName());
    }

    public Builder from(Plan plan) {
      return from(plan.getExtensionUrnsList(), plan.getExtensionsList());
    }

    public Builder from(ExtendedExpression extendedExpression) {
      return from(
          extendedExpression.getExtensionUrnsList(), extendedExpression.getExtensionsList());
    }

    private Builder from(
        List<SimpleExtensionURN> simpleExtensionURNs,
        List<SimpleExtensionDeclaration> simpleExtensionDeclarations) {
      Map<Integer, String> urnMap = new HashMap<>();

      for (SimpleExtensionURN extension : simpleExtensionURNs) {
        urnMap.put(extension.getExtensionUrnAnchor(), extension.getUrn());
      }

      for (SimpleExtensionDeclaration extension : simpleExtensionDeclarations) {
        if (extension.hasExtensionFunction()) {
          SimpleExtensionDeclaration.ExtensionFunction func = extension.getExtensionFunction();
          int reference = func.getFunctionAnchor();
          SimpleExtension.FunctionAnchor anchor = resolveFunctionAnchor(func, urnMap);
          functionMap.put(reference, anchor);
        }

        if (extension.hasExtensionType()) {
          SimpleExtensionDeclaration.ExtensionType type = extension.getExtensionType();
          int reference = type.getTypeAnchor();
          SimpleExtension.TypeAnchor anchor = resolveTypeAnchor(type, urnMap);
          typeMap.put(reference, anchor);
        }
      }

      return this;
    }

    public ImmutableExtensionLookup build() {
      return new ImmutableExtensionLookup(
          Collections.unmodifiableMap(functionMap), Collections.unmodifiableMap(typeMap));
    }
  }
}
