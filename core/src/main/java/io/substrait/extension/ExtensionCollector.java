package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
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

  // start at 0 to make sure functionAnchors start with 1 according to spec
  private int counter = 0;

  /** Creates a collector backed by the default extension collection. */
  public ExtensionCollector() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Creates a collector backed by the given extension collection.
   *
   * @param extensionCollection the extension collection to resolve declarations against, must not
   *     be null
   */
  public ExtensionCollector(SimpleExtension.ExtensionCollection extensionCollection) {
    super(new HashMap<>(), new HashMap<>());
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
  }

  /**
   * Returns the reference for the given function declaration, allocating a new one if the function
   * has not been seen before.
   *
   * @param declaration the function declaration to reference
   * @return the function reference
   */
  public int getFunctionReference(SimpleExtension.Function declaration) {
    Integer i = funcMap.reverseGet(declaration.getAnchor());
    if (i != null) {
      return i;
    }
    ++counter; // prefix here to make clearer than postfixing at end.
    funcMap.put(counter, declaration.getAnchor());
    return counter;
  }

  /**
   * Returns the reference for the given type anchor, allocating a new one if the type has not been
   * seen before.
   *
   * @param typeAnchor the type anchor to reference
   * @return the type reference
   */
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
   * Adds the collected extension URNs and declarations to the given plan builder.
   *
   * @param builder the plan builder to populate
   */
  public void addExtensionsToPlan(Plan.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  /**
   * Adds the collected extension URNs and declarations to the given extended expression builder.
   *
   * @param builder the extended expression builder to populate
   */
  public void addExtensionsToExtendedExpression(ExtendedExpression.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  private SimpleExtensions getExtensions() {
    AtomicInteger urnPos = new AtomicInteger(1);
    HashMap<String, SimpleExtensionURN> urns = new HashMap<>();

    ArrayList<SimpleExtensionDeclaration> extensionList = new ArrayList<>();
    for (Map.Entry<Integer, SimpleExtension.FunctionAnchor> e : funcMap.forwardEntrySet()) {
      String urn = e.getValue().urn();

      SimpleExtensionURN urnObj =
          urns.computeIfAbsent(
              urn,
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());

      SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
          SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
              .setFunctionAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionFunction(funcBuilder).build();
      extensionList.add(decl);
    }

    for (Map.Entry<Integer, SimpleExtension.TypeAnchor> e : typeMap.forwardEntrySet()) {
      String urn = e.getValue().urn();

      SimpleExtensionURN urnObj =
          urns.computeIfAbsent(
              urn,
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());

      SimpleExtensionDeclaration.ExtensionType.Builder typeBuilder =
          SimpleExtensionDeclaration.ExtensionType.newBuilder()
              .setTypeAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(urnObj.getExtensionUrnAnchor());

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionType(typeBuilder).build();
      extensionList.add(decl);
    }
    return new SimpleExtensions(urns, extensionList);
  }

  private static final class SimpleExtensions {
    final HashMap<String, SimpleExtensionURN> urns;
    final ArrayList<SimpleExtensionDeclaration> extensionList;

    SimpleExtensions(
        HashMap<String, SimpleExtensionURN> urns,
        ArrayList<SimpleExtensionDeclaration> extensionList) {
      this.urns = urns;
      this.extensionList = extensionList;
    }
  }
}
