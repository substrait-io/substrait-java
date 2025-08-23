package io.substrait.isthmus.operation;

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
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
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
  public SqlKind visit(Aggregate aggregate, EmptyVisitationContext context)
      throws RuntimeException {
    // Aggregation is a core part of a query (DQL).
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(EmptyScan emptyScan, EmptyVisitationContext context)
      throws RuntimeException {
    // An empty scan is typically the result of a query that returns no rows.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Fetch fetch, EmptyVisitationContext context) throws RuntimeException {
    // Fetch corresponds to LIMIT/OFFSET, a part of a query.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Filter filter, EmptyVisitationContext context) throws RuntimeException {
    // Filter corresponds to a WHERE clause, a part of a query.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Join join, EmptyVisitationContext context) throws RuntimeException {
    // A logical join operation.
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(Set set, EmptyVisitationContext context) throws RuntimeException {
    // Maps Substrait's Set operations to their SQL equivalents.
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
  public SqlKind visit(NamedScan namedScan, EmptyVisitationContext context)
      throws RuntimeException {
    // A scan from a named table is the start of a query.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(LocalFiles localFiles, EmptyVisitationContext context)
      throws RuntimeException {
    // A scan from local files is a type of query input.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Project project, EmptyVisitationContext context) throws RuntimeException {
    // Project corresponds to the SELECT clause of a query.
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Expand expand, EmptyVisitationContext context) throws RuntimeException {
    // Expand is a relational operator used in queries (e.g., for grouping sets).
    return QUERY_KIND;
  }

  @Override
  public SqlKind visit(Sort sort, EmptyVisitationContext context) throws RuntimeException {
    // A sort operation directly maps to ORDER BY.
    return SqlKind.ORDER_BY;
  }

  @Override
  public SqlKind visit(Cross cross, EmptyVisitationContext context) throws RuntimeException {
    // A cross join is a type of join.
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(VirtualTableScan virtualTableScan, EmptyVisitationContext context)
      throws RuntimeException {
    // A virtual table scan corresponds to a VALUES clause.
    return SqlKind.VALUES;
  }

  @Override
  public SqlKind visit(ExtensionLeaf extensionLeaf, EmptyVisitationContext context)
      throws RuntimeException {
    // Unknown extension node.
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(ExtensionSingle extensionSingle, EmptyVisitationContext context)
      throws RuntimeException {
    // Unknown extension node.
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(ExtensionMulti extensionMulti, EmptyVisitationContext context)
      throws RuntimeException {
    // Unknown extension node.
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(ExtensionTable extensionTable, EmptyVisitationContext context)
      throws RuntimeException {
    // Unknown extension node.
    return SqlKind.OTHER;
  }

  @Override
  public SqlKind visit(HashJoin hashJoin, EmptyVisitationContext context) throws RuntimeException {
    // A physical hash join is a type of join.
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(MergeJoin mergeJoin, EmptyVisitationContext context)
      throws RuntimeException {
    // A physical merge join is a type of join.
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(NestedLoopJoin nestedLoopJoin, EmptyVisitationContext context)
      throws RuntimeException {
    // A physical nested loop join is a type of join.
    return SqlKind.JOIN;
  }

  @Override
  public SqlKind visit(
      ConsistentPartitionWindow consistentPartitionWindow, EmptyVisitationContext context)
      throws RuntimeException {
    // This relation represents a window function operation.
    return SqlKind.OVER;
  }

  @Override
  public SqlKind visit(NamedWrite write, EmptyVisitationContext context) throws RuntimeException {
    // DML and CTAS write operations.
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
  public SqlKind visit(ExtensionWrite write, EmptyVisitationContext context)
      throws RuntimeException {
    // Custom DML/DDL operations are best mapped to OTHER or OTHER_DDL.
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(NamedDdl ddl, EmptyVisitationContext context) throws RuntimeException {
    // DDL operations, determined by combining the operation and the object type.
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
  public SqlKind visit(ExtensionDdl ddl, EmptyVisitationContext context) throws RuntimeException {
    // A custom DDL operation.
    return SqlKind.OTHER_DDL;
  }

  @Override
  public SqlKind visit(NamedUpdate update, EmptyVisitationContext context) throws RuntimeException {
    // An update operation is a specific DML type. Note that modern Substrait
    // producers should use NamedWrite with WriteOp.UPDATE instead.
    return SqlKind.UPDATE;
  }
}
