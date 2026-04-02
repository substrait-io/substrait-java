package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpressionTypeProjector;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;

/**
 * Abstract base class for read relations that scan data from various sources. Provides common
 * functionality for schema definition and filtering.
 */
public abstract class AbstractReadRel extends ZeroInputRel implements HasExtension {

  /**
   * Returns the initial schema of the data being read.
   *
   * @return the named struct defining the schema
   */
  public abstract NamedStruct getInitialSchema();

  /**
   * Returns an optional filter expression that must be applied during the read.
   *
   * @return the filter expression, if present
   */
  public abstract Optional<Expression> getFilter();

  /**
   * Returns an optional best-effort filter to apply during the read. If the source doesn't support
   * all operations, this filter may not be applied.
   *
   * @return the best-effort filter expression, if present
   */
  public abstract Optional<Expression> getBestEffortFilter();

  public abstract Optional<MaskExpression> getProjection();

  @Override
  protected final Type.Struct deriveRecordType() {
    Type.Struct base = getInitialSchema().struct();
    return getProjection()
        .map(projection -> MaskExpressionTypeProjector.project(projection, base))
        .orElse(base);
  }
}
