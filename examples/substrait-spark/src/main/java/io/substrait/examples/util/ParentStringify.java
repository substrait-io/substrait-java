package io.substrait.examples.util;

/**
 * Parent class of all stringifiers Created as it seemed there could be a an optimization to share
 * formatting fns between the various stringifiers
 */
public class ParentStringify {

  protected String indentChar = " ";
  protected int indent = 0;
  protected int indentSize = 3;

  /**
   * Build with a specific indent at the start - note 'an indent' is set by default to be 3 spaces.
   *
   * @param indent number of indentes
   */
  public ParentStringify(int indent) {
    this.indent = indent;
  }

  StringBuilder getIndent() {

    var sb = new StringBuilder();
    if (indent != 0) {
      sb.append("\n");
    }
    sb.append(getIndentString());

    indent++;
    return sb;
  }

  StringBuilder getIndentString() {

    var sb = new StringBuilder();
    sb.append(indentChar.repeat(this.indent * this.indentSize));
    sb.append("+- ");
    return sb;
  }

  StringBuilder getContinuationIndentString() {

    var sb = new StringBuilder();
    if (indent != 0) {
      sb.append("\n");
    }
    sb.append(indentChar.repeat(this.indent * this.indentSize));
    sb.append(" : ");
    return sb;
  }

  protected String getOutdent(StringBuilder sb) {
    indent--;
    return (sb).toString();
  }
}
