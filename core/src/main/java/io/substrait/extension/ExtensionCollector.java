package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.SimpleExtensionURN;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a mapping between function/type anchors and function/type references. Generates
 * references for new anchors as they are requested.
 *
 * <p>Used to replace instances of function and types in the POJOs with references when converting
 * from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan}
 */
public class ExtensionCollector extends AbstractExtensionLookup {
  private final BidiMap<Integer, SimpleExtension.FunctionAnchor> funcMap;
  private final BidiMap<Integer, SimpleExtension.TypeAnchor> typeMap;
  private final SimpleExtension.ExtensionCollection extensionCollection;

  // start at 0 to make sure functionAnchors start with 1 according to spec
  private int counter = 0;

  private String getUriFromUrn(String urn) {
    return extensionCollection.getUriFromUrn(urn);
  }

  public ExtensionCollector() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public ExtensionCollector(SimpleExtension.ExtensionCollection extensionCollection) {
    super(new HashMap<>(), new HashMap<>());
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
    this.extensionCollection = extensionCollection;
  }

  public int getFunctionReference(SimpleExtension.Function declaration) {
    Integer i = funcMap.reverseGet(declaration.getAnchor());
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    funcMap.put(counter, declaration.getAnchor());
    return counter;
  }

  public int getTypeReference(SimpleExtension.TypeAnchor typeAnchor) {
    Integer i = typeMap.reverseGet(typeAnchor);
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    typeMap.put(counter, typeAnchor);
    return counter;
  }

  /**
   * Returns an ExtensionCollection containing only the types and functions that have been tracked
   * by this collector. This provides a minimal collection with exactly what was used during
   * serialization.
   *
   * <p>This collection contains:
   *
   * <ul>
   *   <li>Only the types that were referenced via {@link #getTypeReference}
   *   <li>Only the functions that were referenced via {@link #getFunctionReference}
   *   <li>URI/URN mappings for only the used extension URNs
   * </ul>
   *
   * <p>Types from the catalog are resolved, while custom UserDefined types (not in the catalog) are
   * created via {@link SimpleExtension.Type#of(String, String)}.
   *
   * @return an ExtensionCollection with only the used types, functions, and URI/URN mappings
   */
  public SimpleExtension.ExtensionCollection getExtensionCollection() {
    java.util.List<SimpleExtension.Type> types = new ArrayList<>();
    java.util.List<SimpleExtension.ScalarFunctionVariant> scalarFunctions = new ArrayList<>();
    java.util.List<SimpleExtension.AggregateFunctionVariant> aggregateFunctions = new ArrayList<>();
    java.util.List<SimpleExtension.WindowFunctionVariant> windowFunctions = new ArrayList<>();

    java.util.Set<String> usedUrns = new java.util.HashSet<>();

    for (Map.Entry<Integer, SimpleExtension.TypeAnchor> entry : typeMap.forwardEntrySet()) {
      SimpleExtension.TypeAnchor anchor = entry.getValue();
      usedUrns.add(anchor.urn());
      if (extensionCollection.hasType(anchor)) {
        types.add(extensionCollection.getType(anchor));
      } else {
        types.add(SimpleExtension.Type.of(anchor.urn(), anchor.key()));
      }
    }

    for (Map.Entry<Integer, SimpleExtension.FunctionAnchor> entry : funcMap.forwardEntrySet()) {
      SimpleExtension.FunctionAnchor anchor = entry.getValue();
      usedUrns.add(anchor.urn());

      if (extensionCollection.hasScalarFunction(anchor)) {
        scalarFunctions.add(extensionCollection.getScalarFunction(anchor));
      } else if (extensionCollection.hasAggregateFunction(anchor)) {
        aggregateFunctions.add(extensionCollection.getAggregateFunction(anchor));
      } else if (extensionCollection.hasWindowFunction(anchor)) {
        windowFunctions.add(extensionCollection.getWindowFunction(anchor));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Function %s::%s was tracked but not found in catalog as scalar, aggregate, or window function",
                anchor.urn(), anchor.key()));
      }
    }

    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    for (String urn : usedUrns) {
      String uri = extensionCollection.getUriFromUrn(urn);
      if (uri != null) {
        uriUrnMap.put(uri, urn);
      }
    }

    return SimpleExtension.ExtensionCollection.builder()
        .addAllTypes(types)
        .addAllScalarFunctions(scalarFunctions)
        .addAllAggregateFunctions(aggregateFunctions)
        .addAllWindowFunctions(windowFunctions)
        .uriUrnMap(uriUrnMap)
        .build();
  }

  public void addExtensionsToPlan(Plan.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensionUris(simpleExtensions.uris.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  public void addExtensionsToExtendedExpression(ExtendedExpression.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensionUris(simpleExtensions.uris.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  private SimpleExtensions getExtensions() {
    AtomicInteger urnPos = new AtomicInteger(1);
    AtomicInteger uriPos = new AtomicInteger(1);
    HashMap<String, SimpleExtensionURN> urns = new HashMap<>();
    HashMap<String, SimpleExtensionURI> uris = new HashMap<>();

    ArrayList<SimpleExtensionDeclaration> extensionList = new ArrayList<>();
    for (Map.Entry<Integer, SimpleExtension.FunctionAnchor> e : funcMap.forwardEntrySet()) {
      String urn = e.getValue().urn();
      String uri = getUriFromUrn(urn);

      // Create URN entry
      SimpleExtensionURN urnObj =
          urns.computeIfAbsent(
              urn,
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());

      // Create URI entry if mapping exists
      SimpleExtensionURI uriObj = null;
      if (uri != null) {
        uriObj =
            uris.computeIfAbsent(
                uri,
                k ->
                    SimpleExtensionURI.newBuilder()
                        .setExtensionUriAnchor(uriPos.getAndIncrement())
                        .setUri(k)
                        .build());
      }

      // Create function declaration with both URN and URI references
      SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
          SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
              .setFunctionAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      if (uriObj != null) {
        funcBuilder.setExtensionUriReference(uriObj.getExtensionUriAnchor());
      }

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionFunction(funcBuilder).build();
      extensionList.add(decl);
    }

    for (Map.Entry<Integer, SimpleExtension.TypeAnchor> e : typeMap.forwardEntrySet()) {
      String urn = e.getValue().urn();
      String uri = getUriFromUrn(urn);

      // Create URN entry
      SimpleExtensionURN urnObj =
          urns.computeIfAbsent(
              urn,
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());

      // Create URI entry if mapping exists
      SimpleExtensionURI uriObj = null;
      if (uri != null) {
        uriObj =
            uris.computeIfAbsent(
                uri,
                k ->
                    SimpleExtensionURI.newBuilder()
                        .setExtensionUriAnchor(uriPos.getAndIncrement())
                        .setUri(k)
                        .build());
      }

      // Create type declaration with both URN and URI references
      SimpleExtensionDeclaration.ExtensionType.Builder typeBuilder =
          SimpleExtensionDeclaration.ExtensionType.newBuilder()
              .setTypeAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      if (uriObj != null) {
        typeBuilder.setExtensionUriReference(uriObj.getExtensionUriAnchor());
      }

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionType(typeBuilder).build();
      extensionList.add(decl);
    }
    return new SimpleExtensions(urns, uris, extensionList);
  }

  private static final class SimpleExtensions {
    final HashMap<String, SimpleExtensionURN> urns;
    final HashMap<String, SimpleExtensionURI> uris;
    final ArrayList<SimpleExtensionDeclaration> extensionList;

    SimpleExtensions(
        HashMap<String, SimpleExtensionURN> urns,
        HashMap<String, SimpleExtensionURI> uris,
        ArrayList<SimpleExtensionDeclaration> extensionList) {
      this.urns = urns;
      this.uris = uris;
      this.extensionList = extensionList;
    }
  }
}
