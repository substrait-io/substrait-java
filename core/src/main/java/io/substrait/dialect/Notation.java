package io.substrait.dialect;

/** How a system function is written in the system's own syntax. */
public enum Notation {
  /** The function is written between its operands (e.g. {@code a + b}). */
  INFIX,
  /** The function is written after its operand (e.g. {@code a!}). */
  POSTFIX,
  /** The function is written before its operand (e.g. {@code -a}). */
  PREFIX,
  /** The function is written using call syntax (e.g. {@code f(a, b)}). */
  FUNCTION
}
