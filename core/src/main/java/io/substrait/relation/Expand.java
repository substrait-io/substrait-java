package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/**
 * Expands the input relation into a set of projected fields where each field is either consistent
 * or switches among duplicate expressions.
 */
@Value.Enclosing
@Value.Immutable
public abstract class Expand extends SingleInputRel {

  /**
   * Returns the fields produced by the expand operation.
   *
   * @return the list of expand fields
   */
  public abstract List<ExpandField> getFields();

  /**
   * Derives the output record type from the expand fields.
   *
   * @return the resulting struct type
   */
  @Override
  public Type.Struct deriveRecordType() {
    Type.Struct initial = getInput().getRecordType();
    return TypeCreator.of(initial.nullable())
        .struct(getFields().stream().map(ExpandField::getType));
  }

  /**
   * Accepts a relation visitor.
   *
   * @param <O> the result type
   * @param <C> the visitation context type
   * @param <E> the exception type that may be thrown
   * @param visitor the relation visitor
   * @param context the visitation context
   * @return the visit result
   * @throws E if the visitor signals an error
   */
  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Expand}.
   *
   * @return a new immutable builder
   */
  public static ImmutableExpand.Builder builder() {
    return ImmutableExpand.builder();
  }

  /** A field produced by the expand operation. */
  public interface ExpandField {

    /**
     * Returns the field type.
     *
     * @return the type of the field
     */
    Type getType();
  }

  /** A field whose value is defined by a single expression. */
  @Value.Immutable
  public abstract static class ConsistentField implements ExpandField {

    /**
     * Returns the expression that defines the field value.
     *
     * @return the defining expression
     */
    public abstract Expression getExpression();

    /**
     * Returns the type derived from the expression.
     *
     * @return the field type
     */
    @Override
    public Type getType() {
      return getExpression().getType();
    }

    /**
     * Creates a builder for {@link ConsistentField}.
     *
     * @return a new immutable builder
     */
    public static ImmutableExpand.ConsistentField.Builder builder() {
      return ImmutableExpand.ConsistentField.builder();
    }
  }

  /**
   * A field that may switch among duplicate expressions; nullability propagates if any duplicate is
   * nullable.
   */
  @Value.Immutable
  public abstract static class SwitchingField implements ExpandField {

    /**
     * Returns the duplicate expressions this field may switch between.
     *
     * @return list of duplicate expressions
     */
    public abstract List<Expression> getDuplicates();

    /**
     * Returns the type derived from duplicates, adjusting nullability if needed.
     *
     * @return the field type
     */
    @Override
    public Type getType() {
      boolean nullable = getDuplicates().stream().anyMatch(d -> d.getType().nullable());
      Type type = getDuplicates().get(0).getType();
      return nullable ? TypeCreator.asNullable(type) : TypeCreator.asNotNullable(type);
    }

    /**
     * Creates a builder for {@link SwitchingField}.
     *
     * @return a new immutable builder
     */
    public static ImmutableExpand.SwitchingField.Builder builder() {
      return ImmutableExpand.SwitchingField.builder();
    }
  }
}
