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

  public ExtensionCollector() {
    super(new HashMap<>(), new HashMap<>());
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
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

  public void addExtensionsToPlan(Plan.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  public void addExtensionsToExtendedExpression(ExtendedExpression.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(simpleExtensions.urns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  private SimpleExtensions getExtensions() {
    AtomicInteger urnPos = new AtomicInteger(1);
    HashMap<String, SimpleExtensionURN> urns = new HashMap<>();

    ArrayList<SimpleExtensionDeclaration> extensionList = new ArrayList<>();
    for (Map.Entry<Integer, SimpleExtension.FunctionAnchor> e : funcMap.forwardMap.entrySet()) {
      SimpleExtensionURN urn =
          urns.computeIfAbsent(
              e.getValue().urn(),
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());
      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionFunction(
                  SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                      .setFunctionAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUrnReference(urn.getExtensionUrnAnchor()))
              .build();
      extensionList.add(decl);
    }
    for (Map.Entry<Integer, SimpleExtension.TypeAnchor> e : typeMap.forwardMap.entrySet()) {
      SimpleExtensionURN urn =
          urns.computeIfAbsent(
              e.getValue().urn(),
              k ->
                  SimpleExtensionURN.newBuilder()
                      .setExtensionUrnAnchor(urnPos.getAndIncrement())
                      .setUrn(k)
                      .build());
      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionType(
                  SimpleExtensionDeclaration.ExtensionType.newBuilder()
                      .setTypeAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUrnReference(urn.getExtensionUrnAnchor()))
              .build();
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

  /** We don't depend on guava... */
  private static class BidiMap<T1, T2> {
    private final Map<T1, T2> forwardMap;
    private final Map<T2, T1> reverseMap;

    public BidiMap(Map<T1, T2> forwardMap) {
      this.forwardMap = forwardMap;
      this.reverseMap = new HashMap<>();
    }

    public T2 get(T1 t1) {
      return forwardMap.get(t1);
    }

    public T1 reverseGet(T2 t2) {
      return reverseMap.get(t2);
    }

    public void put(T1 t1, T2 t2) {
      forwardMap.put(t1, t2);
      reverseMap.put(t2, t1);
    }
  }
}
