package io.substrait.isthmus.expression;

/** Receives type observations while converting Substrait expressions to Calcite. */
@FunctionalInterface
public interface TypeObserver {
  /** Observer that disables type inference and discards all observations. */
  TypeObserver NOOP = observation -> {};

  /**
   * Receives the result of attempting to observe an expression's inferred type.
   *
   * <p>Exceptions thrown by an observer are propagated to the conversion caller.
   *
   * @param observation supplied type and either an inferred type or inference failure
   */
  void observe(TypeObservation observation);
}
