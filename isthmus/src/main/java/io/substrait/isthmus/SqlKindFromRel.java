package io.substrait.isthmus;

import io.substrait.relation.Aggregate;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionDdl;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.ExtensionWrite;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.util.EmptyVisitationContext;
import org.apache.calcite.sql.SqlKind;

/**
 * A visitor to infer the general SqlKind from the root of a Substrait Rel tree. Note: This infers
 * the general operation type, as the original SQL syntax is not preserved in the Substrait plan.
 */
public class SqlKindFromRel
    implements RelVisitor<SqlKind, EmptyVisitationContext, RuntimeException> {

  // Most common query operations map to SELECT.
  private static final SqlKind QUERY_KIND = SqlKind.SELECT;

  @Override
  public SqlKind visit(final Aggregate aggregate, final EmptyVisitationContext context)
      throws RuntimeException {

    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final EmptyScan emptyScan, final EmptyVisitationContext context)
      throws RuntimeException {
    // An empty scan is typically the result of a query that returns no rows.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Fetch fetch, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Filter filter, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Join join, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(final Set set, final EmptyVisitationContext context)
      throws RuntimeException {
    switch (set.getSetOp()) {
      case UNION_ALL:
      case UNION_DISTINCT:
        return SqlKind.UNION;
      case INTERSECTION_PRIMARY:
      case INTERSECTION_MULTISET:
      case INTERSECTION_MULTISET_ALL:
        return SqlKind.INTERSECT;
      case MINUS_PRIMARY:
      case MINUS_PRIMARY_ALL:
      case MINUS_MULTISET:
        return SqlKind.EXCEPT;
      case UNKNOWN:
      default:
        return SqlKind.OTHER;
    }
  }

  @Override
  public SqlKind visit(final NamedScan namedScan, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final LocalFiles localFiles, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Project project, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Expand expand, final EmptyVisitationContext context)
      throws RuntimeException {
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(final Sort sort, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.ORDER_BY;
  }

  @Override
  public SqlKind visit(final Cross cross, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(
      final VirtualTableScan virtualTableScan, final EmptyVisitationContext context)
      throws RuntimeException {
    // A virtual table scan corresponds to a VALUES clause.
    return SqlKind.VALUES;
  }

  @Override
  public SqlKind visit(final ExtensionLeaf extensionLeaf, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(final ExtensionSingle extensionSingle, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(final ExtensionMulti extensionMulti, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(final ExtensionTable extensionTable, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(final HashJoin hashJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(final MergeJoin mergeJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(final NestedLoopJoin nestedLoopJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(
      final ConsistentPartitionWindow consistentPartitionWindow,
      final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OVER;
  }

  @Override
  public SqlKind visit(final NamedWrite write, final EmptyVisitationContext context)
      throws RuntimeException {
    switch (write.getOperation()) {
      case INSERT:
        return SqlKind.INSERT;
      case DELETE:
        return SqlKind.DELETE;
      case UPDATE:
        return SqlKind.UPDATE;
      case CTAS:
        return SqlKind.CREATE_TABLE;
      default:
        return SqlKind.OTHER;
    }
  }

  @Override
  public SqlKind visit(final ExtensionWrite write, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final NamedDdl ddl, final EmptyVisitationContext context)
      throws RuntimeException {
    switch (ddl.getOperation()) {
      case CREATE:
      case CREATE_OR_REPLACE:
        if (ddl.getObject() == NamedDdl.DdlObject.TABLE) {
          return SqlKind.CREATE_TABLE;
        } else if (ddl.getObject() == NamedDdl.DdlObject.VIEW) {
          return SqlKind.CREATE_VIEW;
        }
        break;
      case DROP:
      case DROP_IF_EXIST:
        if (ddl.getObject() == NamedDdl.DdlObject.TABLE) {
          return SqlKind.DROP_TABLE;
        } else if (ddl.getObject() == NamedDdl.DdlObject.VIEW) {
          return SqlKind.DROP_VIEW;
        }
        break;
      case ALTER:
        if (ddl.getObject() == NamedDdl.DdlObject.TABLE) {
          return SqlKind.ALTER_TABLE;
        } else if (ddl.getObject() == NamedDdl.DdlObject.VIEW) {
          return SqlKind.ALTER_VIEW;
        }
        break;
    }
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final ExtensionDdl ddl, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final NamedUpdate update, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.UPDATE;
  }

  @Override
  public SqlKind visit(final ScatterExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final SingleBucketExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final MultiBucketExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final RoundRobinExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(final BroadcastExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    return SqlKind.OTHER_DDL;
  }
}
