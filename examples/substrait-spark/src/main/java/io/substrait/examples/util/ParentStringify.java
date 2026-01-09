package io.substrait.examples.util;

/**
 * Parent class of all string-ifiers Created as it seemed there could be an optimization to share
 * formatting fns between the various string-ifiers
 */
public class ParentStringify {

  /** Indent character. */
  protected String indentChar = " ";

  /** Number of indents to use. */
  protected int indent;

  /** Size of each indent. */
  protected int indentSize = 3;

  /**
   * Build with a specific indent at the start - note 'an indent' is set by default to be 3 spaces.
   *
   * @param indent Number of indents to use.
   */
  public ParentStringify(int indent) {
    this.indent = indent;
  }

  StringBuilder getIndent() {

    StringBuilder sb = new StringBuilder();
    if (indent != 0) {
      sb.append("\n");
    }
    sb.append(getIndentString());

    indent++;
    return sb;
  }

  StringBuilder getIndentString() {

    StringBuilder sb = new StringBuilder();
    sb.append(indentChar.repeat(this.indent * this.indentSize));
    sb.append("+- ");
    return sb;
  }

  StringBuilder getContinuationIndentString() {

    StringBuilder sb = new StringBuilder();
    if (indent != 0) {
      sb.append("\n");
    }
    sb.append(indentChar.repeat(this.indent * this.indentSize));
    sb.append(" : ");
    return sb;
  }

  /**
   * Get the outdent to use, decrements indent counter.
   *
   * @param sb StringBuilder with outdent
   * @return outdent string
   */
  protected String getOutdent(StringBuilder sb) {
    indent--;
    return (sb).toString();
  }
}
