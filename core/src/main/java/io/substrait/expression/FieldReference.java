package io.substrait.expression;

import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.substrait.util.VisitationContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * An expression that references a field, either of an input relation/expression or of the query
 * root, optionally navigating through nested struct, list and map segments.
 */
@Value.Immutable
public abstract class FieldReference implements Expression {

  /**
   * Returns the reference segments navigating from the root to the referenced field.
   *
   * @return the reference segments
   */
  public abstract List<ReferenceSegment> segments();

  /**
   * Returns the type of the referenced field.
   *
   * @return the field type
   */
  public abstract Type type();

  /**
   * Returns the expression this reference is rooted at, if it is not a root reference.
   *
   * @return the optional input expression
   */
  public abstract Optional<Expression> inputExpression();

  /**
   * Returns the number of subquery levels stepped out of for an outer reference, if applicable.
   *
   * @return the optional number of steps out
   */
  public abstract Optional<Integer> outerReferenceStepsOut();

  /**
   * Returns the number of lambda nesting levels stepped out of for a lambda parameter reference, if
   * applicable.
   *
   * @return the optional number of lambda steps out
   */
  public abstract Optional<Integer> lambdaParameterReferenceStepsOut();

  @Override
  public Type getType() {
    return type();
  }

  /**
   * Creates a builder for {@link FieldReference}.
   *
   * @return a new builder
   */
  public static ImmutableFieldReference.Builder builder() {
    return ImmutableFieldReference.builder();
  }

  @Override
  public <R, C extends VisitationContext, E extends Throwable> R accept(
      ExpressionVisitor<R, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Validates that a field reference is not simultaneously an outer reference and a lambda
   * parameter reference.
   *
   * @throws IllegalArgumentException if both step-out values are set
   */
  @Value.Check
  protected void check() {
    if (outerReferenceStepsOut().isPresent() && lambdaParameterReferenceStepsOut().isPresent()) {
      throw new IllegalArgumentException(
          "FieldReference cannot have both outerReferenceStepsOut and lambdaParameterReferenceStepsOut set");
    }
  }

  /**
   * Returns whether this is a single-segment reference directly into the query root.
   *
   * @return {@code true} if this is a simple root reference
   */
  public boolean isSimpleRootReference() {
    return segments().size() == 1
        && !inputExpression().isPresent()
        && !outerReferenceStepsOut().isPresent()
        && !lambdaParameterReferenceStepsOut().isPresent();
  }

  /**
   * Returns whether this reference steps out into an enclosing (outer) query.
   *
   * @return {@code true} if this is an outer reference
   */
  public boolean isOuterReference() {
    return outerReferenceStepsOut().orElse(0) > 0;
  }

  /**
   * Returns true if this field reference refers to a lambda parameter.
   *
   * @return {@code true} if this is a lambda parameter reference
   */
  public boolean isLambdaParameterReference() {
    return lambdaParameterReferenceStepsOut().isPresent();
  }

  /**
   * Returns a new reference extended to navigate into the struct field at the given index.
   *
   * @param index the struct field index
   * @return the extended field reference
   */
  public FieldReference dereferenceStruct(int index) {
    Type newType = StructFieldFinder.getReferencedType(type(), index);
    return dereference(newType, StructField.of(index));
  }

  private FieldReference dereference(Type newType, ReferenceSegment nextSegment) {
    return ImmutableFieldReference.builder()
        .type(newType)
        .addSegments(nextSegment)
        .addAllSegments(segments())
        .inputExpression(inputExpression())
        .build();
  }

  /**
   * Returns a new reference extended to navigate into the list element at the given index.
   *
   * @param index the list element index
   * @return the extended field reference
   */
  public FieldReference dereferenceList(int index) {
    Type newType = ListIndexFinder.getReferencedType(type(), index);
    return dereference(newType, ListElement.of(index));
  }

  /**
   * Returns a new reference extended to navigate into the map value for the given key.
   *
   * @param mapKey the map key literal
   * @return the extended field reference
   */
  public FieldReference dereferenceMap(Literal mapKey) {
    Type newType = MapKeyFinder.getReferencedType(type(), mapKey.getType());
    return dereference(newType, MapKey.of(mapKey));
  }

  /**
   * Creates a reference to a map value within the given expression.
   *
   * @param mapKey the map key literal
   * @param expression the expression to reference into
   * @return the field reference
   */
  public static FieldReference newMapReference(Literal mapKey, Expression expression) {
    return ImmutableFieldReference.builder()
        .addSegments(MapKey.of(mapKey))
        .inputExpression(expression)
        .type(MapKeyFinder.getReferencedType(expression.getType(), mapKey.getType()))
        .build();
  }

  /**
   * Creates a reference to a list element within the given expression.
   *
   * @param index the list element index
   * @param expression the expression to reference into
   * @return the field reference
   */
  public static FieldReference newListReference(int index, Expression expression) {
    return ImmutableFieldReference.builder()
        .addSegments(ListElement.of(index))
        .inputExpression(expression)
        .type(ListIndexFinder.getReferencedType(expression.getType(), index))
        .build();
  }

  /**
   * Creates a reference to a struct field within the given expression.
   *
   * @param index the struct field index
   * @param expression the expression to reference into
   * @return the field reference
   */
  public static FieldReference newStructReference(int index, Expression expression) {
    return ImmutableFieldReference.builder()
        .addSegments(StructField.of(index))
        .inputExpression(expression)
        .type(StructFieldFinder.getReferencedType(expression.getType(), index))
        .build();
  }

  /**
   * Creates a reference to a field of the query root struct.
   *
   * @param index the struct field index
   * @param knownType the known type of the referenced field
   * @return the field reference
   */
  public static FieldReference newRootStructReference(int index, Type knownType) {
    return ImmutableFieldReference.builder()
        .addSegments(StructField.of(index))
        .type(knownType)
        .build();
  }

  /**
   * Creates a reference to a field of an enclosing (outer) query's root struct.
   *
   * @param index the struct field index
   * @param knownType the known type of the referenced field
   * @param stepsOut the number of subquery levels to step out
   * @return the field reference
   */
  public static FieldReference newRootStructOuterReference(
      int index, Type knownType, int stepsOut) {
    return ImmutableFieldReference.builder()
        .addSegments(StructField.of(index))
        .type(knownType)
        .outerReferenceStepsOut(stepsOut)
        .build();
  }

  /**
   * Creates a reference to a field of a single input relation by overall field index.
   *
   * @param index the field index within the relation's record type
   * @param rel the input relation
   * @return the field reference
   */
  public static FieldReference newInputRelReference(int index, Rel rel) {
    return newInputRelReference(index, Collections.singletonList(rel));
  }

  /**
   * Creates a reference to a field by overall field index across a list of input relations.
   *
   * @param index the field index across the combined record types
   * @param rels the input relations, in field order
   * @return the field reference
   * @throws IllegalArgumentException if the index is beyond the combined field count
   */
  public static FieldReference newInputRelReference(int index, List<Rel> rels) {
    int currentOffset = 0;
    for (Rel r : rels) {
      int relSize = r.getRecordType().fields().size();
      if (index < currentOffset + relSize) {
        Type referenceType = r.getRecordType().fields().get(index - currentOffset);
        return ImmutableFieldReference.builder()
            .addSegments(StructField.of(index))
            .type(referenceType)
            .build();
      }

      currentOffset += relSize;
    }

    throw new IllegalArgumentException(
        String.format(
            "The current index %d wasn't found within the number of fields %d",
            index, currentOffset));
  }

  static FieldReference newLambdaParameterReference(int stepsOut, int paramIndex, Type knownType) {
    return ImmutableFieldReference.builder()
        .addSegments(StructField.of(paramIndex))
        .type(knownType)
        .lambdaParameterReferenceStepsOut(stepsOut)
        .build();
  }

  /**
   * A single navigation step within a {@link FieldReference} (struct field, list element or map
   * key).
   */
  public interface ReferenceSegment {
    /**
     * Extends the given field reference by this segment.
     *
     * @param reference the reference to extend
     * @return the extended field reference
     */
    FieldReference apply(FieldReference reference);

    /**
     * Creates a field reference applying this segment to the given expression.
     *
     * @param expr the expression to reference into
     * @return the field reference
     */
    FieldReference constructOnExpression(Expression expr);

    /**
     * Creates a field reference applying this segment to the given root struct.
     *
     * @param struct the root struct type
     * @return the field reference
     */
    FieldReference constructOnRoot(Type.Struct struct);
  }

  /** A reference segment selecting a struct field by offset. */
  @Value.Immutable
  public abstract static class StructField implements ReferenceSegment {
    /**
     * Returns the zero-based field offset.
     *
     * @return the field offset
     */
    public abstract int offset();

    /**
     * Creates a struct-field segment for the given offset.
     *
     * @param index the field offset
     * @return the struct-field segment
     */
    public static StructField of(int index) {
      return ImmutableStructField.builder().offset(index).build();
    }

    @Override
    public FieldReference apply(FieldReference reference) {
      return reference.dereferenceStruct(offset());
    }

    @Override
    public FieldReference constructOnExpression(Expression expr) {
      return FieldReference.newStructReference(offset(), expr);
    }

    @Override
    public FieldReference constructOnRoot(Type.Struct struct) {
      if (offset() >= struct.fields().size()) {
        throw new IllegalArgumentException(
            String.format(
                "Field reference offset (%s) must be less than number of fields in struct (%s)",
                offset(), struct.fields().size()));
      }
      return FieldReference.newRootStructReference(offset(), struct.fields().get((offset())));
    }
  }

  /** A reference segment selecting a list element by offset. */
  @Value.Immutable
  public abstract static class ListElement implements ReferenceSegment {
    /**
     * Returns the zero-based list element offset.
     *
     * @return the element offset
     */
    public abstract int offset();

    /**
     * Creates a list-element segment for the given offset.
     *
     * @param index the element offset
     * @return the list-element segment
     */
    public static ListElement of(int index) {
      return ImmutableListElement.builder().offset(index).build();
    }

    @Override
    public FieldReference apply(FieldReference reference) {
      return reference.dereferenceList(offset());
    }

    @Override
    public FieldReference constructOnExpression(Expression expr) {
      return FieldReference.newListReference(offset(), expr);
    }

    @Override
    public FieldReference constructOnRoot(Type.Struct struct) {
      throw new UnsupportedOperationException();
    }
  }

  /** A reference segment selecting a map value by key literal. */
  @Value.Immutable
  public abstract static class MapKey implements ReferenceSegment {
    /**
     * Returns the key literal identifying the map entry.
     *
     * @return the key literal
     */
    public abstract Expression.Literal key();

    /**
     * Creates a map-key segment for the given key literal.
     *
     * @param key the key literal
     * @return the map-key segment
     */
    public static MapKey of(Expression.Literal key) {
      return ImmutableMapKey.builder().key(key).build();
    }

    @Override
    public FieldReference apply(FieldReference reference) {
      return reference.dereferenceMap(key());
    }

    @Override
    public FieldReference constructOnExpression(Expression expr) {
      return FieldReference.newMapReference(key(), expr);
    }

    @Override
    public FieldReference constructOnRoot(Type.Struct struct) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Creates a field reference rooted at an expression and navigating through the given segments.
   *
   * @param expression the expression to reference into
   * @param segments the navigation segments, outermost first
   * @return the field reference
   */
  public static FieldReference ofExpression(
      Expression expression, List<ReferenceSegment> segments) {
    return of(null, expression, segments);
  }

  private static FieldReference of(
      Type.Struct struct, Expression expression, List<ReferenceSegment> segments) {
    FieldReference reference = null;
    Collections.reverse(segments);
    for (int i = 0; i < segments.size(); i++) {
      if (i == 0) {
        ReferenceSegment last = segments.get(0);
        reference =
            struct == null ? last.constructOnExpression(expression) : last.constructOnRoot(struct);
      } else {
        reference = segments.get(i).apply(reference);
      }
    }

    return reference;
  }

  /**
   * Creates a field reference rooted at a struct and navigating through the given segments.
   *
   * @param struct the root struct type
   * @param segments the navigation segments, outermost first
   * @return the field reference
   */
  public static FieldReference ofRoot(Type.Struct struct, List<ReferenceSegment> segments) {
    return of(struct, null, segments);
  }

  private static class StructFieldFinder
      extends TypeVisitor.TypeThrowsVisitor<Type, RuntimeException> {

    private final int index;

    private StructFieldFinder(int index) {
      super(
          "This visitor only supports retrieving struct types. Was applied to a non-struct type.");
      this.index = index;
    }

    @Override
    public Type visit(Type.Struct expr) throws RuntimeException {
      if (expr.fields().size() < index) {
        throw new IllegalArgumentException("Undefined struct type.");
      }
      return expr.fields().get(index);
    }

    public static Type getReferencedType(Type type, int index) {
      return type.accept(new StructFieldFinder(index));
    }
  }

  private static class ListIndexFinder
      extends TypeVisitor.TypeThrowsVisitor<Type, RuntimeException> {
    private ListIndexFinder() {
      super(
          "This visitor only supports retrieving array index offsets. Was applied to a non-array type.");
    }

    @Override
    public Type visit(Type.ListType expr) throws RuntimeException {
      return expr.elementType();
    }

    public static Type getReferencedType(Type type, int index) {
      return type.accept(new ListIndexFinder());
    }
  }

  private static class MapKeyFinder extends TypeVisitor.TypeThrowsVisitor<Type, RuntimeException> {

    private final Type keyType;

    private MapKeyFinder(Type keyType) {
      super(
          "This visitor only supports retrieving map values using map keys. Was applied to a non-map type.");
      this.keyType = keyType;
    }

    @Override
    public Type visit(Type.Map expr) throws RuntimeException {
      // TODO: decide whether to support inconsistent map key type literals. Inclined to not.
      if (!(expr.key().equals(keyType))) {
        throw new IllegalArgumentException(
            String.format(
                "Key type %s of map does not matched expected type of %s.", expr.key(), keyType));
      }
      return expr.value();
    }

    public static Type getReferencedType(Type typeToDereference, Type keyType) {
      return typeToDereference.accept(new MapKeyFinder(keyType));
    }
  }
}
