package io.substrait.util;

/**
 * Marker interface for the context object threaded through the relation and expression visitors
 * (for example {@code RelVisitor} and {@code ExpressionVisitor}).
 *
 * <p>Visitors are parameterized by a context type {@code C extends VisitationContext}, allowing
 * callers to carry arbitrary state through a traversal. Implementations provide whatever state a
 * particular visitor needs.
 */
public interface VisitationContext {}
