package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.FieldReference.ListElement;
import io.substrait.expression.FieldReference.MapKey;
import io.substrait.expression.FieldReference.ReferenceSegment;
import io.substrait.expression.FieldReference.StructField;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins {@link FieldReference#resolveType} against the reference-building factories it mirrors.
 *
 * <p>{@code resolveType} reports the type a chain of segments selects without throwing when it
 * selects nothing, while {@link FieldReference#ofRoot} and {@link FieldReference#ofExpression}
 * throw in that case. The two must agree on <em>which</em> chains select something, so that a
 * caller re-deriving a cached type can tell a reference that no longer resolves from a failure of
 * its own work. Asserting the equivalence rather than hard-coding expectations is what keeps {@code
 * resolveType} from drifting away from the segment derivation rules it duplicates.
 */
class FieldReferenceResolveTypeTest extends TestBase {

  /** Segments are held innermost first, the order {@link FieldReference#segments()} uses. */
  private static List<ReferenceSegment> segments(ReferenceSegment... innermostFirst) {
    return Arrays.asList(innermostFirst);
  }

  private static MapKey key(String value) {
    return MapKey.of(Expression.StrLiteral.builder().value(value).build());
  }

  static Stream<Arguments> cases() {
    return Stream.of(
        // struct field, single segment
        Arguments.of("struct field in range", R.struct(R.I64), segments(StructField.of(0))),
        Arguments.of(
            "struct field, second column", R.struct(R.I64, R.STRING), segments(StructField.of(1))),
        Arguments.of("struct field past the end", R.struct(R.I64), segments(StructField.of(2))),
        Arguments.of(
            "struct field at the field count", R.struct(R.I64), segments(StructField.of(1))),
        Arguments.of("negative struct field", R.struct(R.I64), segments(StructField.of(-1))),
        // struct field, nested
        Arguments.of(
            "nested struct field in range",
            R.struct(R.struct(R.I64, R.STRING)),
            segments(StructField.of(1), StructField.of(0))),
        Arguments.of(
            "nested struct field past the end",
            R.struct(R.struct(R.I64)),
            segments(StructField.of(1), StructField.of(0))),
        Arguments.of(
            "three struct fields deep, in range",
            R.struct(R.struct(R.struct(R.I64, R.STRING))),
            segments(StructField.of(1), StructField.of(0), StructField.of(0))),
        Arguments.of(
            "three struct fields deep, innermost gone",
            R.struct(R.struct(R.struct(R.I64))),
            segments(StructField.of(1), StructField.of(0), StructField.of(0))),
        // container kind mismatches under a struct field
        Arguments.of(
            "struct field on a list",
            R.struct(R.list(R.I64)),
            segments(StructField.of(0), StructField.of(0))),
        Arguments.of(
            "struct field on a map",
            R.struct(R.map(R.STRING, R.I64)),
            segments(StructField.of(0), StructField.of(0))),
        Arguments.of(
            "struct field on a scalar",
            R.struct(R.I64),
            segments(StructField.of(0), StructField.of(0))),
        // list element
        Arguments.of(
            "list element on a list",
            R.struct(R.list(R.I64)),
            segments(ListElement.of(0), StructField.of(0))),
        Arguments.of(
            "list element offset is not bounds checked",
            R.struct(R.list(R.I64)),
            segments(ListElement.of(7), StructField.of(0))),
        Arguments.of(
            "list element on a struct",
            R.struct(R.struct(R.I64)),
            segments(ListElement.of(0), StructField.of(0))),
        Arguments.of(
            "list element as the outermost segment", R.struct(R.I64), segments(ListElement.of(0))),
        // map key
        Arguments.of(
            "map key matching the key type",
            R.struct(R.map(R.STRING, R.I64)),
            segments(key("k"), StructField.of(0))),
        Arguments.of(
            "map key differing in nullability",
            R.struct(R.map(N.STRING, R.I64)),
            segments(key("k"), StructField.of(0))),
        Arguments.of(
            "map key of the wrong type",
            R.struct(R.map(R.I64, R.I64)),
            segments(key("k"), StructField.of(0))),
        Arguments.of(
            "map key on a list", R.struct(R.list(R.I64)), segments(key("k"), StructField.of(0))),
        Arguments.of("map key as the outermost segment", R.struct(R.I64), segments(key("k"))),
        // degenerate
        Arguments.of("no segments", R.struct(R.I64), Collections.<ReferenceSegment>emptyList()));
  }

  /**
   * Resolves via {@link FieldReference#ofRoot}, mapping its failure modes — every exception it can
   * throw, and the null it returns for an empty segment chain — onto "selects nothing".
   */
  private static Optional<Type> viaOfRoot(Type.Struct rootType, List<ReferenceSegment> segments) {
    try {
      FieldReference reference = FieldReference.ofRoot(rootType, new ArrayList<>(segments));
      return reference == null ? Optional.empty() : Optional.of(reference.type());
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  /**
   * The same, via {@link FieldReference#ofExpression} rooted at an expression of {@code rootType}.
   */
  private static Optional<Type> viaOfExpression(Type rootType, List<ReferenceSegment> segments) {
    Expression root = FieldReference.newRootStructReference(0, rootType);
    try {
      FieldReference reference = FieldReference.ofExpression(root, new ArrayList<>(segments));
      return reference == null ? Optional.empty() : Optional.of(reference.type());
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void agreesWithOfRoot(String name, Type.Struct rootType, List<ReferenceSegment> segments) {
    assertEquals(viaOfRoot(rootType, segments), FieldReference.resolveType(rootType, segments));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void agreesWithOfExpression(String name, Type.Struct rootType, List<ReferenceSegment> segments) {
    assertEquals(
        viaOfExpression(rootType, segments), FieldReference.resolveType(rootType, segments));
  }

  @Test
  void resolvesAgainstRootsThatAreNotStructs() {
    // ofRoot only accepts a struct, but a reference rooted at an expression can navigate into a
    // list or a map directly, so resolveType has to accept any type as the root.
    assertEquals(
        Optional.of(R.I64), FieldReference.resolveType(R.list(R.I64), segments(ListElement.of(0))));
    assertEquals(
        Optional.of(R.I64), FieldReference.resolveType(R.map(R.STRING, R.I64), segments(key("k"))));
    assertFalse(FieldReference.resolveType(R.I64, segments(StructField.of(0))).isPresent());
  }

  @Test
  void doesNotModifyTheGivenSegments() {
    // ofRoot and ofExpression reverse the list they are given in place, which is why they cannot be
    // handed FieldReference.segments() directly. resolveType must not have that requirement.
    List<ReferenceSegment> segments =
        Collections.unmodifiableList(
            Arrays.<ReferenceSegment>asList(StructField.of(1), StructField.of(0)));

    assertEquals(
        Optional.of(R.STRING),
        FieldReference.resolveType(R.struct(R.struct(R.I64, R.STRING)), segments));
    assertEquals(StructField.of(1), segments.get(0));
    assertEquals(StructField.of(0), segments.get(1));
  }

  @Test
  void resolvesTheSegmentsOfAReferenceItselfUnchanged() {
    // The end the whole method exists for: taking segments() straight off a reference and resolving
    // them against a record type, with no defensive copy at the call site.
    FieldReference reference =
        FieldReference.newRootStructReference(0, R.struct(R.I64, R.STRING)).dereferenceStruct(1);

    assertEquals(2, reference.segments().size());
    assertEquals(
        Optional.of(R.STRING),
        FieldReference.resolveType(R.struct(R.struct(R.I64, R.STRING)), reference.segments()));
    assertTrue(
        FieldReference.resolveType(R.struct(R.struct(R.I64, R.STRING)), reference.segments())
            .isPresent());
    assertEquals(2, reference.segments().size());
  }
}
