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

  private String getUriFromUrn(final String urn) {
    return extensionCollection.getUriFromUrn(urn);
  }

  public ExtensionCollector() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public ExtensionCollector(final SimpleExtension.ExtensionCollection extensionCollection) {
    super(new HashMap<>(), new HashMap<>());
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
    this.extensionCollection = extensionCollection;
  }

  public int getFunctionReference(final SimpleExtension.Function declaration) {
    final Integer i = funcMap.reverseGet(declaration.getAnchor());
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    funcMap.put(counter, declaration.getAnchor());
    return counter;
  }

  public int getTypeReference(final SimpleExtension.TypeAnchor typeAnchor) {
    final Integer i = typeMap.reverseGet(typeAnchor);
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    typeMap.put(counter, typeAnchor);
    return counter;
  }

  public void addExtensionsToPlan(final Plan.Builder builder) {
    final SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensionUris(simpleExtensions.uris.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  public void addExtensionsToExtendedExpression(final ExtendedExpression.Builder builder) {
    final SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensionUris(simpleExtensions.uris.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  private SimpleExtensions getExtensions() {
    final AtomicInteger urnPos = new AtomicInteger(1);
    final AtomicInteger uriPos = new AtomicInteger(1);
    final HashMap<String, SimpleExtensionURN> urns = new HashMap<>();
    final HashMap<String, SimpleExtensionURI> uris = new HashMap<>();

    final ArrayList<SimpleExtensionDeclaration> extensionList = new ArrayList<>();
    for (final Map.Entry<Integer, SimpleExtension.FunctionAnchor> e : funcMap.forwardEntrySet()) {
      final String urn = e.getValue().urn();
      final String uri = getUriFromUrn(urn);

      // Create URN entry
      final SimpleExtensionURN urnObj =
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
      final SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
          SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
              .setFunctionAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      if (uriObj != null) {
        funcBuilder.setExtensionUriReference(uriObj.getExtensionUriAnchor());
      }

      final SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionFunction(funcBuilder).build();
      extensionList.add(decl);
    }

    for (final Map.Entry<Integer, SimpleExtension.TypeAnchor> e : typeMap.forwardEntrySet()) {
      final String urn = e.getValue().urn();
      final String uri = getUriFromUrn(urn);

      // Create URN entry
      final SimpleExtensionURN urnObj =
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
      final SimpleExtensionDeclaration.ExtensionType.Builder typeBuilder =
          SimpleExtensionDeclaration.ExtensionType.newBuilder()
              .setTypeAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      if (uriObj != null) {
        typeBuilder.setExtensionUriReference(uriObj.getExtensionUriAnchor());
      }

      final SimpleExtensionDeclaration decl =
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
        final HashMap<String, SimpleExtensionURN> urns,
        final HashMap<String, SimpleExtensionURI> uris,
        final ArrayList<SimpleExtensionDeclaration> extensionList) {
      this.urns = urns;
      this.uris = uris;
      this.extensionList = extensionList;
    }
  }
}
