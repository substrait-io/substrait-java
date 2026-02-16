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

/**
 * A generic visitor for {@link RelNode} trees that supports custom return types and checked
 * exceptions.
 *
 * <p>provides type-safe methods for common Calcite relational operators and a fallback for
 * unhandled types. It is useful when implementing transformations or analysis logic over relational
 * expressions without extending Calcite's built-in visitor classes.
 *
 * @param <OUTPUT> the return type of visitor methods
 * @param <EXCEPTION> the checked exception type that may be thrown during visiting
 */
public abstract class RelNodeVisitor<OUTPUT, EXCEPTION extends Throwable> {
  /**
   * Visits a {@link TableScan} node.
   *
   * @param scan the table scan node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(TableScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  /**
   * Visits a {@link TableFunctionScan} node.
   *
   * @param scan the table function scan node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(TableFunctionScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  /**
   * Visits a {@link Values} node.
   *
   * @param values the values node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Values values) throws EXCEPTION {
    return visitOther(values);
  }

  /**
   * Visits a {@link Filter} node.
   *
   * @param filter the filter node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Filter filter) throws EXCEPTION {
    return visitOther(filter);
  }

  /**
   * Visits a {@link Calc} node.
   *
   * @param calc the calc node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Calc calc) throws EXCEPTION {
    return visitOther(calc);
  }

  /**
   * Visits a {@link Project} node.
   *
   * @param project the project node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Project project) throws EXCEPTION {
    return visitOther(project);
  }

  /**
   * Visits a {@link Join} node.
   *
   * @param join the join node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Join join) throws EXCEPTION {
    return visitOther(join);
  }

  /**
   * Visits a {@link Correlate} node.
   *
   * @param correlate the correlate node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Correlate correlate) throws EXCEPTION {
    return visitOther(correlate);
  }

  /**
   * Visits a {@link Union} node.
   *
   * @param union the union node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Union union) throws EXCEPTION {
    return visitOther(union);
  }

  /**
   * Visits an {@link Intersect} node.
   *
   * @param intersect the intersect node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Intersect intersect) throws EXCEPTION {
    return visitOther(intersect);
  }

  /**
   * Visits a {@link Minus} node.
   *
   * @param minus the minus node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Minus minus) throws EXCEPTION {
    return visitOther(minus);
  }

  /**
   * Visits an {@link Aggregate} node.
   *
   * @param aggregate the aggregate node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Aggregate aggregate) throws EXCEPTION {
    return visitOther(aggregate);
  }

  /**
   * Visits a {@link Match} node.
   *
   * @param match the match node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Match match) throws EXCEPTION {
    return visitOther(match);
  }

  /**
   * Visits a {@link Sort} node.
   *
   * @param sort the sort node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Sort sort) throws EXCEPTION {
    return visitOther(sort);
  }

  /**
   * Visits an {@link Exchange} node.
   *
   * @param exchange the exchange node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(Exchange exchange) throws EXCEPTION {
    return visitOther(exchange);
  }

  /**
   * Visits a {@link TableModify} node.
   *
   * @param modify the table modify node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public OUTPUT visit(TableModify modify) throws EXCEPTION {
    return visitOther(modify);
  }

  /**
   * Fallback method for visiting any {@link RelNode} type not explicitly handled.
   *
   * @param other the relational node
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public abstract OUTPUT visitOther(RelNode other) throws EXCEPTION;

  /** Protected constructor to enforce subclassing. */
  protected RelNodeVisitor() {}

  /**
   * Dispatches to the appropriate visit method based on the runtime type of the {@link RelNode}.
   *
   * <p>Use this instead of {@code RelNode.accept(visitor)} because {@link RelNodeVisitor} cannot
   * extend Calcite's base visitor classes.
   *
   * @param node the relational node to visit
   * @return the result of visiting this node
   * @throws EXCEPTION if an error occurs during processing
   */
  public final OUTPUT reverseAccept(RelNode node) throws EXCEPTION {
    if (node instanceof TableScan) {
      return this.visit((TableScan) node);
    } else if (node instanceof TableFunctionScan) {
      return this.visit((TableFunctionScan) node);
    } else if (node instanceof Values) {
      return this.visit((Values) node);
    } else if (node instanceof Filter) {
      return this.visit((Filter) node);
    } else if (node instanceof Calc) {
      return this.visit((Calc) node);
    } else if (node instanceof Project) {
      return this.visit((Project) node);
    } else if (node instanceof Join) {
      return this.visit((Join) node);
    } else if (node instanceof Correlate) {
      return this.visit((Correlate) node);
    } else if (node instanceof Union) {
      return this.visit((Union) node);
    } else if (node instanceof Intersect) {
      return this.visit((Intersect) node);
    } else if (node instanceof Minus) {
      return this.visit((Minus) node);
    } else if (node instanceof Match) {
      return this.visit((Match) node);
    } else if (node instanceof Sort) {
      return this.visit((Sort) node);
    } else if (node instanceof Exchange) {
      return this.visit((Exchange) node);
    } else if (node instanceof Aggregate) {
      return this.visit((Aggregate) node);
    } else if (node instanceof TableModify) {
      return this.visit((TableModify) node);
    } else {
      return this.visitOther(node);
    }
  }
}
