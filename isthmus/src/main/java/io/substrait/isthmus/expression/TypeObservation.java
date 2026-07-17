package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The result of attempting to independently infer a Calcite type during expression conversion.
 * Exactly one of {@link #inferredType()} and {@link #inferenceFailure()} is present.
 */
public final class TypeObservation {
  /** The expression category that produced an observation. */
  public enum Source {
    /** A scalar function invocation. */
    SCALAR_FUNCTION
  }

  private final Source source;
  private final Expression expression;
  private final RelDataType inferredType;
  private final RuntimeException inferenceFailure;

  /**
   * Creates a successful type observation.
   *
   * @param source expression category that produced the observation
   * @param expression expression that produced the observation
   * @param inferredType type independently inferred by Calcite
   * @return a successful type observation
   */
  static TypeObservation success(Source source, Expression expression, RelDataType inferredType) {
    return new TypeObservation(source, expression, inferredType, null);
  }

  /**
   * Creates a failed type observation.
   *
   * @param source expression category that produced the observation
   * @param expression expression that produced the observation
   * @param inferenceFailure failure to independently infer a Calcite type
   * @return a failed type observation
   */
  static TypeObservation failure(
      Source source, Expression expression, RuntimeException inferenceFailure) {
    return new TypeObservation(source, expression, null, inferenceFailure);
  }

  private TypeObservation(
      Source source,
      Expression expression,
      RelDataType inferredType,
      RuntimeException inferenceFailure) {
    this.source = Objects.requireNonNull(source, "source");
    this.expression = Objects.requireNonNull(expression, "expression");
    if ((inferredType == null) == (inferenceFailure == null)) {
      throw new IllegalArgumentException(
          "Exactly one of inferredType and inferenceFailure must be present");
    }
    this.inferredType = inferredType;
    this.inferenceFailure = inferenceFailure;
  }

  /**
   * Returns the expression category that produced this observation.
   *
   * @return the expression category
   */
  public Source source() {
    return source;
  }

  /**
   * Returns the expression that produced this observation.
   *
   * @return the observed expression
   */
  public Expression expression() {
    return expression;
  }

  /**
   * Returns the type supplied by Substrait.
   *
   * @return the supplied type
   */
  public Type suppliedType() {
    return expression.getType();
  }

  /**
   * Returns the type independently inferred by Calcite.
   *
   * @return the inferred type, or empty if inference failed
   */
  public Optional<RelDataType> inferredType() {
    return Optional.ofNullable(inferredType);
  }

  /**
   * Returns the Calcite inference failure.
   *
   * @return the inference failure, or empty if inference succeeded
   */
  public Optional<RuntimeException> inferenceFailure() {
    return Optional.ofNullable(inferenceFailure);
  }
}
