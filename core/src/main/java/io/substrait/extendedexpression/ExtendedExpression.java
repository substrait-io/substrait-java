package io.substrait.extendedexpression;

import io.substrait.expression.Expression;
import io.substrait.proto.AdvancedExtension;
import io.substrait.relation.Aggregate;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Represents an extended expression that references multiple expressions and schema details.
 *
 * <p>Includes references to expressions, expected type URLs, and optional advanced extensions.
 */
@Value.Immutable
public abstract class ExtendedExpression {

  /**
   * Returns the list of referred expression references.
   *
   * @return list of expression references
   */
  public abstract List<ExpressionReferenceBase> getReferredExpressions();

  /**
   * Returns the base schema associated with this extended expression.
   *
   * @return the base schema
   */
  public abstract NamedStruct getBaseSchema();

  /**
   * Returns the expected type URLs for validation.
   *
   * @return list of expected type URLs
   */
  public abstract List<String> getExpectedTypeUrls();

  /**
   * Returns the optional advanced extension metadata.
   *
   * @return optional advanced extension
   */
  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  /**
   * Creates a builder for {@link ExtendedExpression}.
   *
   * @return a new builder
   */
  public static ImmutableExtendedExpression.Builder builder() {
    return ImmutableExtendedExpression.builder();
  }

  /** Base interface for expression references. */
  public interface ExpressionReferenceBase {
    /**
     * Returns the output names associated with this reference.
     *
     * @return list of output names
     */
    List<String> getOutputNames();
  }

  /** Represents a reference to a single expression. */
  @Value.Immutable
  public abstract static class ExpressionReference implements ExpressionReferenceBase {
    /**
     * Returns the referenced expression.
     *
     * @return the expression
     */
    public abstract Expression getExpression();

    /**
     * Creates a builder for {@link ExpressionReference}.
     *
     * @return a new builder
     */
    public static ImmutableExpressionReference.Builder builder() {
      return ImmutableExpressionReference.builder();
    }
  }

  /** Represents a reference to an aggregate function measure. */
  @Value.Immutable
  public abstract static class AggregateFunctionReference implements ExpressionReferenceBase {
    /**
     * Returns the referenced aggregate measure.
     *
     * @return the measure
     */
    public abstract Aggregate.Measure getMeasure();
  }
}
