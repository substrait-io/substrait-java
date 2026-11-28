package io.substrait.extension;

import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
  private final List<SimpleExtensionURN> retainedUrns;
  private final List<SimpleExtensionDeclaration> retainedDeclarations;
  private final int highestRetainedAnchor;
  private int counter;

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
    this(Collections.emptyList(), Collections.emptyList());
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
  }

  /**
   * Creates a collector that preserves the original URNs and declarations, including those used
   * only by opaque subtrees. New function and type anchors start above the highest retained
   * function or type anchor. URN anchors have a separate counter.
   *
   * @param urns the original extension URNs
   * @param declarations the original extension declarations
   */
  public ExtensionCollector(
      List<SimpleExtensionURN> urns, List<SimpleExtensionDeclaration> declarations) {
    super(new HashMap<>(), new HashMap<>());
    retainedUrns = new ArrayList<>(urns);
    retainedDeclarations = new ArrayList<>(declarations);
    if (retainedDeclarations.isEmpty()) {
      highestRetainedAnchor = 0;
    } else {
      ImmutableExtensionLookup lookup =
          ImmutableExtensionLookup.builder().from(retainedUrns, retainedDeclarations).build();
      functionAnchorMap.putAll(lookup.functionAnchorMap);
      typeAnchorMap.putAll(lookup.typeAnchorMap);
      highestRetainedAnchor =
          Stream.concat(functionAnchorMap.keySet().stream(), typeAnchorMap.keySet().stream())
              .max(Integer::compareUnsigned)
              .orElse(0);
    }
    funcMap = new BidiMap<>(functionAnchorMap);
    typeMap = new BidiMap<>(typeAnchorMap);
    counter = highestRetainedAnchor;
  }

  private static int nextAnchor(int anchor) {
    if (anchor == -1) {
      throw new IllegalStateException(
          "Cannot allocate an extension anchor above the uint32 maximum");
    }
    return anchor + 1;
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
    counter = nextAnchor(counter);
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
    counter = nextAnchor(counter);
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

    builder.addAllExtensionUrns(retainedUrns);
    builder.addAllExtensionUrns(simpleExtensions.newUrns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  /**
   * Adds the collected extension URNs and declarations to the given extended expression builder.
   *
   * @param builder the extended expression builder to populate
   */
  public void addExtensionsToExtendedExpression(ExtendedExpression.Builder builder) {
    SimpleExtensions simpleExtensions = getExtensions();

    builder.addAllExtensionUrns(retainedUrns);
    builder.addAllExtensionUrns(simpleExtensions.newUrns.values());
    builder.addAllExtensions(simpleExtensions.extensionList);
  }

  private SimpleExtensions getExtensions() {
    SimpleExtensions extensions = new SimpleExtensions(retainedUrns, retainedDeclarations);
    for (Map.Entry<Integer, SimpleExtension.FunctionAnchor> e : funcMap.forwardEntrySet()) {
      if (Integer.compareUnsigned(e.getKey(), highestRetainedAnchor) <= 0) {
        continue;
      }
      SimpleExtensionDeclaration.ExtensionFunction.Builder funcBuilder =
          SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
              .setFunctionAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(extensions.urnReference(e.getValue().urn()));

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionFunction(funcBuilder).build();
      extensions.extensionList.add(decl);
    }

    for (Map.Entry<Integer, SimpleExtension.TypeAnchor> e : typeMap.forwardEntrySet()) {
      if (Integer.compareUnsigned(e.getKey(), highestRetainedAnchor) <= 0) {
        continue;
      }
      SimpleExtensionDeclaration.ExtensionType.Builder typeBuilder =
          SimpleExtensionDeclaration.ExtensionType.newBuilder()
              .setTypeAnchor(e.getKey())
              .setName(e.getValue().key())
              .setExtensionUrnReference(extensions.urnReference(e.getValue().urn()));

      SimpleExtensionDeclaration decl =
          SimpleExtensionDeclaration.newBuilder().setExtensionType(typeBuilder).build();
      extensions.extensionList.add(decl);
    }
    return extensions;
  }

  private static final class SimpleExtensions {
    final Map<String, Integer> retainedUrnAnchors = new HashMap<>();
    final Map<String, SimpleExtensionURN> newUrns = new HashMap<>();
    final List<SimpleExtensionDeclaration> extensionList;
    int highestUrnAnchor;

    SimpleExtensions(List<SimpleExtensionURN> urns, List<SimpleExtensionDeclaration> declarations) {
      extensionList = new ArrayList<>(declarations);
      for (SimpleExtensionURN urn : urns) {
        int anchor = urn.getExtensionUrnAnchor();
        retainedUrnAnchors.put(urn.getUrn(), anchor);
        if (Integer.compareUnsigned(anchor, highestUrnAnchor) > 0) {
          highestUrnAnchor = anchor;
        }
      }
    }

    int urnReference(String urn) {
      Integer retained = retainedUrnAnchors.get(urn);
      if (retained != null) {
        return retained;
      }
      return newUrns.computeIfAbsent(urn, this::newUrn).getExtensionUrnAnchor();
    }

    private SimpleExtensionURN newUrn(String urn) {
      highestUrnAnchor = nextAnchor(highestUrnAnchor);
      return SimpleExtensionURN.newBuilder()
          .setExtensionUrnAnchor(highestUrnAnchor)
          .setUrn(urn)
          .build();
    }
  }
}
