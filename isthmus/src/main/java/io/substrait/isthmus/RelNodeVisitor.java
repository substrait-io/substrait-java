package io.substrait.isthmus;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;

/** A more generic version of RelShuttle that allows an alternative return value. */
public abstract class RelNodeVisitor<OUTPUT, EXCEPTION extends Throwable> {

  public OUTPUT visit(TableScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  public OUTPUT visit(TableFunctionScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  public OUTPUT visit(Values values) throws EXCEPTION {
    return visitOther(values);
  }

  public OUTPUT visit(Filter filter) throws EXCEPTION {
    return visitOther(filter);
  }

  public OUTPUT visit(Calc calc) throws EXCEPTION {
    return visitOther(calc);
  }

  public OUTPUT visit(Project project) throws EXCEPTION {
    return visitOther(project);
  }

  public OUTPUT visit(Join join) throws EXCEPTION {
    return visitOther(join);
  }

  public OUTPUT visit(Correlate correlate) throws EXCEPTION {
    return visitOther(correlate);
  }

  public OUTPUT visit(Union union) throws EXCEPTION {
    return visitOther(union);
  }

  public OUTPUT visit(Intersect intersect) throws EXCEPTION {
    return visitOther(intersect);
  }

  public OUTPUT visit(Minus minus) throws EXCEPTION {
    return visitOther(minus);
  }

  public OUTPUT visit(Aggregate aggregate) throws EXCEPTION {
    return visitOther(aggregate);
  }

  public OUTPUT visit(Match match) throws EXCEPTION {
    return visitOther(match);
  }

  public OUTPUT visit(Sort sort) throws EXCEPTION {
    return visitOther(sort);
  }

  public OUTPUT visit(Exchange exchange) throws EXCEPTION {
    return visitOther(exchange);
  }

  public OUTPUT visit(TableModify modify) throws EXCEPTION {
    return visitOther(modify);
  }

  public OUTPUT visit(Window window) throws EXCEPTION {
    return visitOther(window);
  }

  public abstract OUTPUT visitOther(RelNode other) throws EXCEPTION;

  protected RelNodeVisitor() {}

  /**
   * The method you call when you would normally call RelNode.accept(visitor). Instead, call
   * RelVisitor.reverseAccept(RelNode) due to the lack of ability to extend base classes.
   */
  public final OUTPUT reverseAccept(RelNode node) throws EXCEPTION {
    if (node instanceof TableScan scan) {
      return this.visit(scan);
    } else if (node instanceof TableFunctionScan scan) {
      return this.visit(scan);
    } else if (node instanceof Values values) {
      return this.visit(values);
    } else if (node instanceof Filter filter) {
      return this.visit(filter);
    } else if (node instanceof Calc calc) {
      return this.visit(calc);
    } else if (node instanceof Project project) {
      return this.visit(project);
    } else if (node instanceof Join join) {
      return this.visit(join);
    } else if (node instanceof Correlate correlate) {
      return this.visit(correlate);
    } else if (node instanceof Union union) {
      return this.visit(union);
    } else if (node instanceof Intersect intersect) {
      return this.visit(intersect);
    } else if (node instanceof Minus minus) {
      return this.visit(minus);
    } else if (node instanceof Match match) {
      return this.visit(match);
    } else if (node instanceof Sort sort) {
      return this.visit(sort);
    } else if (node instanceof Exchange exchange) {
      return this.visit(exchange);
    } else if (node instanceof Aggregate aggregate) {
      return this.visit(aggregate);
    } else if (node instanceof TableModify modify) {
      return this.visit(modify);
    } else if (node instanceof Window window) {
      return this.visit(window);
    } else {
      return this.visitOther(node);
    }
  }
}
