package io.substrait.extension;

import com.github.bsideup.jabel.Desugar;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a mapping between function/type anchors and function/type references. Generates
 * references for new anchors as they are requested.
 *
 * <p>Used to replace instances of function and types in the POJOs with references when converting
 * from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan}
 */
public class ExtensionCollector extends AbstractExtensionLookup {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExtensionCollector.class);

  private final BidiMap<Integer, SimpleExtension.FunctionAnchor> funcMap;
  private final BidiMap<Integer, SimpleExtension.TypeAnchor> typeMap;
  private final BidiMap<Integer, String> uriMap;

  private int counter = -1;

  public ExtensionCollector() {
    super(new HashMap<>(), new HashMap<>());
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
    uriMap = new BidiMap<>(new HashMap<>());
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

    builder.addAllExtensionUris(simpleExtensions.uris().values());
    builder.addAllExtensions(simpleExtensions.extensionList());
  }

  public void addExtensionsToExtendedExpression(ExtendedExpression.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUris(simpleExtensions.uris().values());
    builder.addAllExtensions(simpleExtensions.extensionList());
  }

  private SimpleExtensions getExtensions() {
    var uriPos = new AtomicInteger(1);
    var uris = new HashMap<String, SimpleExtensionURI>();

    var extensionList = new ArrayList<SimpleExtensionDeclaration>();
    for (var e : funcMap.forwardMap.entrySet()) {
      SimpleExtensionURI uri =
          uris.computeIfAbsent(
              e.getValue().namespace(),
              k ->
                  SimpleExtensionURI.newBuilder()
                      .setExtensionUriAnchor(uriPos.getAndIncrement())
                      .setUri(k)
                      .build());
      var decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionFunction(
                  SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                      .setFunctionAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUriReference(uri.getExtensionUriAnchor()))
              .build();
      extensionList.add(decl);
    }
    for (var e : typeMap.forwardMap.entrySet()) {
      SimpleExtensionURI uri =
          uris.computeIfAbsent(
              e.getValue().namespace(),
              k ->
                  SimpleExtensionURI.newBuilder()
                      .setExtensionUriAnchor(uriPos.getAndIncrement())
                      .setUri(k)
                      .build());
      var decl =
          SimpleExtensionDeclaration.newBuilder()
              .setExtensionType(
                  SimpleExtensionDeclaration.ExtensionType.newBuilder()
                      .setTypeAnchor(e.getKey())
                      .setName(e.getValue().key())
                      .setExtensionUriReference(uri.getExtensionUriAnchor()))
              .build();
      extensionList.add(decl);
    }
    return new SimpleExtensions(uris, extensionList);
  }

  @Desugar
  private record SimpleExtensions(
      HashMap<String, SimpleExtensionURI> uris,
      ArrayList<SimpleExtensionDeclaration> extensionList) {}

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
