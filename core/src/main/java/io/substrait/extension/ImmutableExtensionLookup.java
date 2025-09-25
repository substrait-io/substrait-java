package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
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
    return new Builder();
  }

  public static class Builder {
    private final Map<Integer, SimpleExtension.FunctionAnchor> functionMap = new HashMap<>();
    private final Map<Integer, SimpleExtension.TypeAnchor> typeMap = new HashMap<>();

    public Builder from(Plan plan, BidiMap<String, String> uriUrnMap) {
      return from(
          plan.getExtensionUrnsList(), plan.getExtensionUrisList(), plan.getExtensionsList(), uriUrnMap);
    }

    public Builder from(ExtendedExpression extendedExpression, BidiMap<String, String> uriUrnMap) {
      return from(
          extendedExpression.getExtensionUrnsList(),
          extendedExpression.getExtensionUrisList(),
          extendedExpression.getExtensionsList(),
          uriUrnMap);
    }

    private Builder from(
        List<SimpleExtensionURN> simpleExtensionURNs,
        List<SimpleExtensionURI> simpleExtensionURIs,
        List<SimpleExtensionDeclaration> simpleExtensionDeclarations,
        BidiMap<String, String> uriUrnMap) {
      Map<Integer, String> urnMap = new HashMap<>();
      Map<Integer, String> uriMap = new HashMap<>();
      // Handle URN format
      for (SimpleExtensionURN extension : simpleExtensionURNs) {
        urnMap.put(extension.getExtensionUrnAnchor(), extension.getUrn());
      }

      for (SimpleExtensionURI extension : simpleExtensionURIs) {
        uriMap.put(extension.getExtensionUriAnchor(), extension.getUri());
      }

      // Add all functions used in plan to the functionMap
      for (SimpleExtensionDeclaration extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionFunction()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionFunction func = extension.getExtensionFunction();
        int reference = func.getFunctionAnchor();
        String urn = urnMap.get(func.getExtensionUrnReference());
        if (urn == null) {
          int uriReference = func.getExtensionUriReference();
          String uri = uriMap.get(uriReference);
          if (uri == null) {
            throw new IllegalStateException(
                "Could not find extension URN for function reference "
                    + func.getExtensionUrnReference()
                    + " or extension URI for function reference "
                    + func.getExtensionUriReference());
          }
          // Translate URI to URN using the BidiMap
          urn = uriUrnMap.get(uri);
          if (urn == null) {
            throw new IllegalStateException(
                "Could not translate URI '" + uri + "' to URN. "
                    + "URI-URN mapping not found in the provided mapping.");
          }
        }
        String name = func.getName();
        SimpleExtension.FunctionAnchor anchor = SimpleExtension.FunctionAnchor.of(urn, name);
        functionMap.put(reference, anchor);
      }

      // Add all types used in plan to the typeMap
      for (SimpleExtensionDeclaration extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionType()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionType type = extension.getExtensionType();
        int reference = type.getTypeAnchor();
        String urn = urnMap.get(type.getExtensionUrnReference());
        if (urn == null) {
          int uriReference = type.getExtensionUriReference();
          String uri = uriMap.get(uriReference);
          if (uri == null) {
            throw new IllegalStateException(
                "Could not find extension URN for type reference "
                    + type.getExtensionUrnReference()
                    + " or extension URI for type reference "
                    + type.getExtensionUriReference());
          }
          // Translate URI to URN using the BidiMap
          urn = uriUrnMap.get(uri);
          if (urn == null) {
            throw new IllegalStateException(
                "Could not translate URI '" + uri + "' to URN. "
                    + "URI-URN mapping not found in the provided mapping.");
          }
        }
        String name = type.getName();
        SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(urn, name);
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
