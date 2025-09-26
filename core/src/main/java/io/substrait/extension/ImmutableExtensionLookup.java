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
    return new Builder();
  }

  public static Builder builder(BidiMap<String, String> uriUrnMap) {
    return new Builder(uriUrnMap);
  }

  public static class Builder {
    private final Map<Integer, SimpleExtension.FunctionAnchor> functionMap = new HashMap<>();
    private final Map<Integer, SimpleExtension.TypeAnchor> typeMap = new HashMap<>();
    private final BidiMap<String, String> uriUrnMap;

    public Builder() {
      this(new BidiMap<>());
    }

    public Builder(BidiMap<String, String> uriUrnMap) {
      this.uriUrnMap = uriUrnMap;
    }

    /**
     * Resolves URN from URI using the URI/URN mapping if available.
     *
     * @param uri The URI to resolve
     * @return The corresponding URN, or null if no mapping exists
     */
    private String resolveUrnFromUri(String uri) {
      return uriUrnMap.get(uri);
    }

    private SimpleExtension.FunctionAnchor resolveFunctionAnchor(
        SimpleExtensionDeclaration.ExtensionFunction func,
        Map<Integer, String> urnMap,
        Map<Integer, String> uriMap) {

      // 1. Try non-zero URN reference
      if (func.getExtensionUrnReference() != 0) {
        String urnFromUrnRef = urnMap.get(func.getExtensionUrnReference());
        if (urnFromUrnRef != null) {
          return SimpleExtension.FunctionAnchor.of(urnFromUrnRef, func.getName());
        }
      }

      // 2. Try non-zero URI reference
      if (func.getExtensionUriReference() != 0) {
        String uriFromUriRef = uriMap.get(func.getExtensionUriReference());
        if (uriFromUriRef != null) {
          String urnFromUriRef = resolveUrnFromUri(uriFromUriRef);
          if (urnFromUriRef != null) {
            return SimpleExtension.FunctionAnchor.of(urnFromUriRef, func.getName());
          }
          // URI found but could not be resolved to URN
        }
      }

      /* At this point both URI and URN are 0, so we need to
        first see if they both resolve.
      */

      String urn = urnMap.get(func.getExtensionUrnReference());
      String uri = uriMap.get(func.getExtensionUriReference());

      // 3. Try both 0 URI and 0 URN if both resolve
      if (uri != null && urn != null) {
        if (urn == resolveUrnFromUri(uri)) {
          return SimpleExtension.FunctionAnchor.of(urn, func.getName());
        }
        // TODO: better exception
        throw new IllegalStateException(
            String.format(
                "0 reference for URI %s and URN %s but URI resolved to differing URN %s",
                uri, urn, resolveUrnFromUri(uri)));
      }

      // 4. Try only 0 URN
      if (urn != null) {
        return SimpleExtension.FunctionAnchor.of(urn, func.getName());
      }
      // 5. Try only 0 URI
      if (uri != null && resolveUrnFromUri(uri) != null) {
        return SimpleExtension.FunctionAnchor.of(resolveUrnFromUri(uri), func.getName());
      }
      throw new IllegalStateException(
          String.format(
              "All resolution strategies failed for URI %s and URN %s (perhaps a URI <-> URN mapping was not registered during the migration) ",
              uri, urn));
    }

    private SimpleExtension.TypeAnchor resolveTypeAnchor(
        SimpleExtensionDeclaration.ExtensionType type,
        Map<Integer, String> urnMap,
        Map<Integer, String> uriMap) {

      // 1. Try non-zero URN reference
      if (type.getExtensionUrnReference() != 0) {
        String urnFromUrnRef = urnMap.get(type.getExtensionUrnReference());
        if (urnFromUrnRef != null) {
          return SimpleExtension.TypeAnchor.of(urnFromUrnRef, type.getName());
        }
      }

      // 2. Try non-zero URI reference
      if (type.getExtensionUriReference() != 0) {
        String uriFromUriRef = uriMap.get(type.getExtensionUriReference());
        if (uriFromUriRef != null) {
          String urnFromUriRef = resolveUrnFromUri(uriFromUriRef);
          if (urnFromUriRef != null) {
            return SimpleExtension.TypeAnchor.of(urnFromUriRef, type.getName());
          }
          // URI found but could not be resolved to URN
        }
      }

      /* At this point both URI and URN are 0, so we need to
        first see if they both resolve.
      */

      String urn = urnMap.get(type.getExtensionUrnReference());
      String uri = uriMap.get(type.getExtensionUriReference());

      // 3. Try both 0 URI and 0 URN if both resolve
      if (uri != null && urn != null) {
        if (urn == resolveUrnFromUri(uri)) {
          return SimpleExtension.TypeAnchor.of(urn, type.getName());
        }
        // TODO: better exception
        throw new IllegalStateException(
            String.format(
                "0 reference for URI %s and URN %s but URI resolved to differing URN %s",
                uri, urn, resolveUrnFromUri(uri)));
      }

      // 4. Try only 0 URN
      if (urn != null) {
        return SimpleExtension.TypeAnchor.of(urn, type.getName());
      }
      // 5. Try only 0 URI
      if (uri != null && resolveUrnFromUri(uri) != null) {
        return SimpleExtension.TypeAnchor.of(resolveUrnFromUri(uri), type.getName());
      }
      throw new IllegalStateException(
          String.format(
              "All resolution strategies failed for URI %s and URN %s (perhaps a URI <-> URN mapping was not registered during the migration) ",
              uri, urn));
    }

    public Builder from(Plan plan) {
      return from(
          plan.getExtensionUrnsList(), plan.getExtensionUrisList(), plan.getExtensionsList());
    }

    public Builder from(ExtendedExpression extendedExpression) {
      return from(
          extendedExpression.getExtensionUrnsList(),
          extendedExpression.getExtensionUrisList(),
          extendedExpression.getExtensionsList());
    }

    private Builder from(
        List<SimpleExtensionURN> simpleExtensionURNs,
        List<io.substrait.proto.SimpleExtensionURI> simpleExtensionURIs,
        List<SimpleExtensionDeclaration> simpleExtensionDeclarations) {
      Map<Integer, String> urnMap = new HashMap<>();
      Map<Integer, String> uriMap = new HashMap<>();

      // Handle URN format
      for (SimpleExtensionURN extension : simpleExtensionURNs) {
        urnMap.put(extension.getExtensionUrnAnchor(), extension.getUrn());
      }

      // Handle deprecated URI format
      for (io.substrait.proto.SimpleExtensionURI extension : simpleExtensionURIs) {
        uriMap.put(extension.getExtensionUriAnchor(), extension.getUri());
      }

      // Add all functions used in plan to the functionMap
      for (SimpleExtensionDeclaration extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionFunction()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionFunction func = extension.getExtensionFunction();
        int reference = func.getFunctionAnchor();
        SimpleExtension.FunctionAnchor anchor = resolveFunctionAnchor(func, urnMap, uriMap);
        functionMap.put(reference, anchor);
      }

      // Add all types used in plan to the typeMap
      for (SimpleExtensionDeclaration extension : simpleExtensionDeclarations) {
        if (!extension.hasExtensionType()) {
          continue;
        }
        SimpleExtensionDeclaration.ExtensionType type = extension.getExtensionType();
        int reference = type.getTypeAnchor();
        SimpleExtension.TypeAnchor anchor = resolveTypeAnchor(type, urnMap, uriMap);
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
