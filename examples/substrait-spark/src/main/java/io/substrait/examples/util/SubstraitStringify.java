package io.substrait.examples.util;

import io.substrait.relation.Aggregate;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Cross;
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

  private boolean showRemap = false;

  public SubstraitStringify() {
    super(0);
  }

  /**
   * Explains the Sustrait plan
   *
   * @param plan Subsrait plan
   * @return List of strings; typically these would then be logged or sent to stdout
   */
  public static List<String> explain(io.substrait.plan.Plan plan) {
    ArrayList<String> explanations = new ArrayList<String>();
    explanations.add("<Substrait Plan>");

    plan.getRoots()
        .forEach(
            root -> {
              Rel rel = root.getInput();

              explanations.add("Root::  " + rel.getClass().getSimpleName() + " " + root.getNames());
              explanations.addAll(explain(rel));
            });

    return explanations;
  }

  /**
   * Explains the Sustrait relation
   *
   * @param rel Subsrait relation
   * @return List of strings; typically these would then be logged or sent to stdout
   */
  public static List<String> explain(io.substrait.relation.Rel rel) {
    SubstraitStringify s = new SubstraitStringify();

    List<String> explanation = new ArrayList<String>();
    explanation.add("<Substrait Relation>");
    explanation.addAll(Arrays.asList(rel.accept(s, EmptyVisitationContext.INSTANCE).split("\n")));
    return explanation;
  }

  private List<String> fieldList(List<io.substrait.type.Type> fields) {
    return fields.stream().map(t -> t.accept(new TypeStringify(0))).collect(Collectors.toList());
  }

  private String getRemap(Rel rel) {
    if (!showRemap) {
      return "";
    }
    int fieldCount = rel.getRecordType().fields().size();
    Optional<Remap> remap = rel.getRemap();
    List<String> recordType = fieldList(rel.getRecordType().fields());

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
  public String visit(Aggregate aggregate, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Aggregate:: ").append(getRemap(aggregate));
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
  public String visit(Fetch fetch, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("Fetch:: ");
    // sb.append(fetch.accept(this));
    return getOutdent(sb);
  }

  @Override
  public String visit(Filter filter, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Filter:: ").append(getRemap(filter));
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
  public String visit(Join join, EmptyVisitationContext context) throws RuntimeException {

    StringBuilder sb =
        getIndent().append("Join:: ").append(join.getJoinType()).append(" ").append(getRemap(join));

    if (join.getCondition().isPresent()) {
      sb.append(join.getCondition().get().accept(new ExpressionStringify(indent), context));
    }

    sb.append(join.getLeft().accept(this, context));
    sb.append(join.getRight().accept(this, context));

    return getOutdent(sb);
  }

  @Override
  public String visit(Set set, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Set:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NamedScan namedScan, EmptyVisitationContext context) throws RuntimeException {

    StringBuilder sb = getIndent().append("NamedScan:: ").append(getRemap(namedScan));
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

  private String namedStruct(NamedStruct struct) {
    StringBuilder sb = new StringBuilder();

    List<String> names = struct.names();
    List<String> types = fieldList(struct.struct().fields());

    for (int x = 0; x < names.size(); x++) {
      if (x != 0) {
        sb.append(",");
      }
      sb.append(names.get(x)).append("[").append(types.get(x)).append("]");
    }

    return sb.toString();
  }

  @Override
  public String visit(LocalFiles localFiles, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("LocalFiles:: ");

    for (FileOrFiles i : localFiles.getItems()) {
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
  public String visit(Project project, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Project:: ").append(getRemap(project));

    sb.append(fieldList(project.deriveRecordType().fields()));

    List<Rel> inputs = project.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this, context));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(Sort sort, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Sort:: ").append(getRemap(sort));
    sort.getSortFields()
        .forEach(
            sf -> {
              ExpressionStringify expr = new ExpressionStringify(indent);
              sb.append(sf.expr().accept(expr, context)).append(" ").append(sf.direction());
            });
    List<Rel> inputs = sort.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this, context));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(Cross cross, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("Cross:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(VirtualTableScan virtualTableScan, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("VirtualTableScan:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionLeaf extensionLeaf, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionLeaf:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionSingle extensionSingle, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionSingle:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionMulti extensionMulti, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionMulti:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionTable extensionTable, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionTable:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(HashJoin hashJoin, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("hashJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(MergeJoin mergeJoin, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("mergeJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NestedLoopJoin nestedLoopJoin, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("nestedLoopJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(
      ConsistentPartitionWindow consistentPartitionWindow, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("consistentPartitionWindow:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(Expand expand, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("expand:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NamedWrite write, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("namedWrite:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionWrite write, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionWrite:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NamedDdl ddl, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("namedDdl:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionDdl ddl, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionDdl:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NamedUpdate update, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = getIndent().append("namedUpdate:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ScatterExchange exchange, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("scatterExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(SingleBucketExchange exchange, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("singleBucketExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(MultiBucketExchange exchange, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("multiBucketExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(RoundRobinExchange exchange, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("roundRobinExchange:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(BroadcastExchange exchange, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = getIndent().append("broadcastExchange:: ");
    return getOutdent(sb);
  }
}
