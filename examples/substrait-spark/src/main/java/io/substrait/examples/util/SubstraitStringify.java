package io.substrait.examples.util;

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
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.type.NamedStruct;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * SubstraitStringify produces a string format output of the Substrait plan or relation
 *
 * <p>This is intended for debug and development purposes only, and follows a similar style to the
 * `explain` API in libraries suck as Spark Calcite etc.
 *
 * <p>Usage:
 *
 * <pre>
 * io.substrait.plan.Plan plan = toSubstrait.convert(enginePlan);
 * SubstraitStringify.explain(plan).forEach(System.out::println);
 * </pre>
 *
 * There is scope for improving this output; there are some gaps in the lesser used relations This
 * is not a replacement for any canoncial form and is only for ease of debugging
 *
 * <p>TODO: https://github.com/substrait-io/substrait-java/issues/302 which tracks the full
 * implementation of this
 */
public class SubstraitStringify extends ParentStringify
    implements RelVisitor<String, EmptyVisitationContext, RuntimeException> {

  private final boolean showRemap = false;

  public SubstraitStringify() {
    super(0);
  }

  /**
   * Explains the Sustrait plan
   *
   * @param plan Subsrait plan
   * @return List of strings; typically these would then be logged or sent to stdout
   */
  public static List<String> explain(final io.substrait.plan.Plan plan) {
    final ArrayList<String> explanations = new ArrayList<String>();
    explanations.add("<Substrait Plan>");

    plan.getRoots()
        .forEach(
            root -> {
              final Rel rel = root.getInput();

              explanations.add("Root::  " + rel.getClass().getSimpleName() + " " + root.getNames());
              explanations.addAll(explain(rel));
            });

    return explanations;
  }

  /**
   * Explains the Sustrait relation
   *
   * @param plan Subsrait relation
   * @return List of strings; typically these would then be logged or sent to stdout
   */
  public static List<String> explain(final io.substrait.relation.Rel rel) {
    final SubstraitStringify s = new SubstraitStringify();

    final List<String> explanation = new ArrayList<String>();
    explanation.add("<Substrait Relation>");
    explanation.addAll(Arrays.asList(rel.accept(s, EmptyVisitationContext.INSTANCE).split("\n")));
    return explanation;
  }

  private List<String> fieldList(final List<io.substrait.type.Type> fields) {
    return fields.stream().map(t -> t.accept(new TypeStringify(0))).collect(Collectors.toList());
  }

  private String getRemap(final Rel rel) {
    if (!showRemap) {
      return "";
    }
    final int fieldCount = rel.getRecordType().fields().size();
    final Optional<Remap> remap = rel.getRemap();
    final List<String> recordType = fieldList(rel.getRecordType().fields());

    if (remap.isPresent()) {
      return "/Remapping fields ("
          + fieldCount
          + ") "
          + remap.get().indices()
          + " as "
          + recordType
          + "/ ";
    } else {
      return "/No Remap (" + fieldCount + ") " + recordType + "/ ";
    }
  }

  @Override
  public String visit(final Aggregate aggregate, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("Aggregate:: ").append(getRemap(aggregate));
    aggregate
        .getGroupings()
        .forEach(
            g -> {
              g.getExpressions()
                  .forEach(
                      expr -> {
                        sb.append(expr.accept(new ExpressionStringify(this.indent), context));
                      });
            });
    aggregate
        .getInputs()
        .forEach(
            s -> {
              sb.append(s.accept(this, context));
            });
    aggregate.getRemap().ifPresent(s -> sb.append(s.toString()));

    return getOutdent(sb);
  }

  @Override
  public String visit(final EmptyScan emptyScan, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("EmptyScan:: ").append(getRemap(emptyScan));
    // sb.append(emptyScan.accept(this));
    return getOutdent(sb);
  }

  @Override
  public String visit(final Fetch fetch, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("Fetch:: ");
    // sb.append(fetch.accept(this));
    return getOutdent(sb);
  }

  @Override
  public String visit(final Filter filter, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("Filter:: ").append(getRemap(filter));
    // .append("{ ");
    sb.append(
        filter.getCondition().accept(new ExpressionStringify(indent), context)) /* .append(")") */;
    filter
        .getInputs()
        .forEach(
            i -> {
              sb.append(i.accept(this, context));
            });

    return getOutdent(sb);
  }

  @Override
  public String visit(final Join join, final EmptyVisitationContext context)
      throws RuntimeException {

    final StringBuilder sb =
        getIndent().append("Join:: ").append(join.getJoinType()).append(" ").append(getRemap(join));

    if (join.getCondition().isPresent()) {
      sb.append(join.getCondition().get().accept(new ExpressionStringify(indent), context));
    }

    sb.append(join.getLeft().accept(this, context));
    sb.append(join.getRight().accept(this, context));

    return getOutdent(sb);
  }

  @Override
  public String visit(final Set set, final EmptyVisitationContext context) throws RuntimeException {
    final StringBuilder sb = getIndent().append("Set:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final NamedScan namedScan, final EmptyVisitationContext context)
      throws RuntimeException {

    final StringBuilder sb = getIndent().append("NamedScan:: ").append(getRemap(namedScan));
    namedScan
        .getInputs()
        .forEach(
            i -> {
              sb.append(i.accept(this, context));
            });
    sb.append(" Tables=");
    sb.append(namedScan.getNames());
    sb.append(" Fields=");
    sb.append(namedStruct(namedScan.getInitialSchema()));
    return getOutdent(sb);
  }

  private String namedStruct(final NamedStruct struct) {
    final StringBuilder sb = new StringBuilder();

    final List<String> names = struct.names();
    final List<String> types = fieldList(struct.struct().fields());

    for (int x = 0; x < names.size(); x++) {
      if (x != 0) {
        sb.append(",");
      }
      sb.append(names.get(x)).append("[").append(types.get(x)).append("]");
    }

    return sb.toString();
  }

  @Override
  public String visit(final LocalFiles localFiles, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("LocalFiles:: ");

    for (final FileOrFiles i : localFiles.getItems()) {
      sb.append(getContinuationIndentString());
      String fileFormat = "";
      if (i.getFileFormat().isPresent()) {
        fileFormat = i.getFileFormat().get().toString();
      }

      sb.append(
          String.format(
              "%s %s len=%d partition=%d start=%d",
              fileFormat, i.getPath().get(), i.getLength(), i.getPartitionIndex(), i.getStart()));
    }

    return getOutdent(sb);
  }

  @Override
  public String visit(final Project project, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("Project:: ").append(getRemap(project));

    sb.append(fieldList(project.deriveRecordType().fields()));

    final List<Rel> inputs = project.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this, context));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(final Sort sort, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("Sort:: ").append(getRemap(sort));
    sort.getSortFields()
        .forEach(
            sf -> {
              final ExpressionStringify expr = new ExpressionStringify(indent);
              sb.append(sf.expr().accept(expr, context)).append(" ").append(sf.direction());
            });
    final List<Rel> inputs = sort.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this, context));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(final Cross cross, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("Cross:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final VirtualTableScan virtualTableScan, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("VirtualTableScan:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionLeaf extensionLeaf, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionLeaf:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionSingle extensionSingle, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionSingle:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionMulti extensionMulti, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionMulti:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionTable extensionTable, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionTable:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final HashJoin hashJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("hashJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final MergeJoin mergeJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("mergeJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final NestedLoopJoin nestedLoopJoin, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("nestedLoopJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(
      final ConsistentPartitionWindow consistentPartitionWindow,
      final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("consistentPartitionWindow:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final Expand expand, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("expand:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final NamedWrite write, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("namedWrite:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionWrite write, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionWrite:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final NamedDdl ddl, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("namedDdl:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ExtensionDdl ddl, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("extensionDdl:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final NamedUpdate update, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("namedUpdate:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final ScatterExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("scatterExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final SingleBucketExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("singleBucketExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final MultiBucketExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("multiBucketExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final RoundRobinExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("roundRobinExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(final BroadcastExchange exchange, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = getIndent().append("broadcastExchange:: ");
    return getOutdent(sb);
  }
}
