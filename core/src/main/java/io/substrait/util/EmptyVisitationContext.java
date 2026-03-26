package io.substrait.util;

/**
 * A singleton implementation of {@link VisitationContext} that provides no additional context. This
 * is useful as a default context when no specific visitation state needs to be maintained.
 */
public class EmptyVisitationContext implements VisitationContext {
  /** Singleton instance of the empty visitation context. */
  public static final EmptyVisitationContext INSTANCE = new EmptyVisitationContext();
}
