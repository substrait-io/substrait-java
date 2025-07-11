package io.substrait.util;

public class EmptyVisitationContext implements VisitationContext {
  public static final EmptyVisitationContext INSTANCE = new EmptyVisitationContext();
}
