package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

/**
 * Represents an update operation on a table.
 *
 * <p>Defines the target schema, update condition, and column transformations.
 */
public abstract class AbstractUpdate extends ZeroInputRel implements HasExtension {

  /**
   * Returns the schema of the target table.
   *
   * @return table schema as {@link NamedStruct}
   */
  public abstract NamedStruct getTableSchema();

  /**
   * Returns the condition that determines which rows to update.
   *
   * @return update condition expression
   */
  public abstract Expression getCondition();

  /**
   * Returns the list of column transformations to apply.
   *
   * @return list of {@link TransformExpression}
   */
  public abstract List<TransformExpression> getTransformations();

  /** Represents a transformation applied to a specific column during an update. */
  @Value.Immutable
  public abstract static class TransformExpression {

    /**
     * Returns the expression that computes the new value for the column.
     *
     * @return transformation expression
     */
    public abstract Expression getTransformation();

    /**
     * Returns the index of the target column to update.
     *
     * @return column index
     */
    public abstract int getColumnTarget();

    /**
     * Creates a builder for {@link TransformExpression}.
     *
     * @return builder instance
     */
    public static ImmutableTransformExpression.Builder builder() {
      return ImmutableTransformExpression.builder();
    }
  }

  /**
   * Derives the output record type from the table schema.
   *
   * @return {@link Type.Struct} based on {@link #getTableSchema()}
   */
  @Override
  public Type.Struct deriveRecordType() {
    return getTableSchema().struct();
  }
}
