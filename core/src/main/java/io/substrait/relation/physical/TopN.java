package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.proto.FetchMode;
import io.substrait.relation.HasExtension;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A physical relation that combines a sort and a fetch, retaining only the number of records
 * required to produce a limited, ordered output. Optionally supports {@code WITH TIES} semantics
 * via its {@link Mode}.
 */
@Value.Immutable
public abstract class TopN extends SingleInputRel implements HasExtension {

  /**
   * Returns the sort fields defining the ordering, applied in order of precedence. At least one
   * sort field is required.
   *
   * @return the sort fields
   */
  public abstract List<Expression.SortField> getSortFields();

  /**
   * Returns the expression evaluating to the number of leading rows to skip. When empty, no rows
   * are skipped (equivalent to an offset of 0).
   *
   * @return the optional offset expression
   */
  public abstract Optional<Expression> getOffset();

  /**
   * Returns the expression evaluating to the maximum number of rows to return. When empty, all
   * remaining rows are returned.
   *
   * @return the optional row count expression
   */
  public abstract Optional<Expression> getCount();

  /**
   * Returns the tie-handling mode for rows tied with the last returned row. Defaults to {@link
   * Mode#ROWS_ONLY}.
   *
   * @return the fetch mode
   */
  @Value.Default
  public Mode getMode() {
    return Mode.ROWS_ONLY;
  }

  /**
   * Determines how a {@link TopN} handles rows tied with the last returned row. Only the modes a
   * producer may set are represented; an unspecified proto mode is normalized to {@link #ROWS_ONLY}
   * on conversion (see {@link #fromProto}).
   */
  public enum Mode {
    /** Return only the requested number of rows. */
    ROWS_ONLY(FetchMode.FETCH_MODE_ROWS_ONLY),
    /** Include additional rows tied with the last row, per the sort fields. */
    WITH_TIES(FetchMode.FETCH_MODE_WITH_TIES);

    private final FetchMode proto;

    Mode(FetchMode proto) {
      this.proto = proto;
    }

    /**
     * Returns the protobuf representation of this mode.
     *
     * @return the proto fetch mode
     */
    public FetchMode toProto() {
      return proto;
    }

    /**
     * Returns the {@link Mode} matching the given protobuf fetch mode. An unspecified proto mode
     * ({@code FETCH_MODE_UNSPECIFIED}) carries no tie-handling semantics and is normalized to the
     * spec default, {@link #ROWS_ONLY}.
     *
     * @param proto the proto fetch mode
     * @return the matching mode
     * @throws IllegalArgumentException if the mode is not recognized
     */
    public static Mode fromProto(FetchMode proto) {
      if (proto == FetchMode.FETCH_MODE_UNSPECIFIED) {
        return ROWS_ONLY;
      }
      for (Mode v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown mode: " + proto);
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link TopN}.
   *
   * @return a new builder
   */
  public static ImmutableTopN.Builder builder() {
    return ImmutableTopN.builder();
  }
}
