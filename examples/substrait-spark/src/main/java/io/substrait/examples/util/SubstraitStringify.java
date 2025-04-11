package io.substrait.examples.util;

import io.substrait.relation.Aggregate;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.NamedStruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    implements RelVisitor<String, RuntimeException> {

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
    var explanations = new ArrayList<String>();
    explanations.add("<Substrait Plan>");

    plan.getRoots()
        .forEach(
            root -> {
              var rel = root.getInput();

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
  public static List<String> explain(io.substrait.relation.Rel rel) {
    var s = new SubstraitStringify();

    List<String> explanation = new ArrayList<String>();
    explanation.add("<Substrait Relation>");
    explanation.addAll(Arrays.asList(rel.accept(s).split("\n")));
    return explanation;
  }

  private boolean showRemap = false;

  private List<String> fieldList(List<io.substrait.type.Type> fields) {
    return fields.stream().map(t -> t.accept(new TypeStringify(0))).collect(Collectors.toList());
  }

  private String getRemap(Rel rel) {
    if (!showRemap) {
      return "";
    }
    var fieldCount = rel.getRecordType().fields().size();
    var remap = rel.getRemap();
    var recordType = fieldList(rel.getRecordType().fields());

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
  public String visit(Aggregate aggregate) throws RuntimeException {
    StringBuilder sb = getIndent().append("Aggregate:: ").append(getRemap(aggregate));
    aggregate
        .getGroupings()
        .forEach(
            g -> {
              g.getExpressions()
                  .forEach(
                      expr -> {
                        sb.append(expr.accept(new ExpressionStringify(this.indent)));
                      });
            });
    aggregate
        .getInputs()
        .forEach(
            s -> {
              sb.append(s.accept(this));
            });
    aggregate.getRemap().ifPresent(s -> sb.append(s.toString()));

    return getOutdent(sb);
  }

  @Override
  public String visit(EmptyScan emptyScan) throws RuntimeException {
    var sb = new StringBuilder("EmptyScan:: ").append(getRemap(emptyScan));
    // sb.append(emptyScan.accept(this));
    return getOutdent(sb);
  }

  @Override
  public String visit(Fetch fetch) throws RuntimeException {
    var sb = new StringBuilder("Fetch:: ");
    // sb.append(fetch.accept(this));
    return getOutdent(sb);
  }

  @Override
  public String visit(Filter filter) throws RuntimeException {
    var sb = getIndent().append("Filter:: ").append(getRemap(filter));
    // .append("{ ");
    sb.append(filter.getCondition().accept(new ExpressionStringify(indent))) /* .append(")") */;
    filter
        .getInputs()
        .forEach(
            i -> {
              sb.append(i.accept(this));
            });

    return getOutdent(sb);
  }

  @Override
  public String visit(Join join) throws RuntimeException {

    var sb =
        getIndent().append("Join:: ").append(join.getJoinType()).append(" ").append(getRemap(join));

    if (join.getCondition().isPresent()) {
      sb.append(join.getCondition().get().accept(new ExpressionStringify(indent)));
    }

    sb.append(join.getLeft().accept(this));
    sb.append(join.getRight().accept(this));

    return getOutdent(sb);
  }

  @Override
  public String visit(Set set) throws RuntimeException {
    StringBuilder sb = getIndent().append("Set:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NamedScan namedScan) throws RuntimeException {

    StringBuilder sb = getIndent().append("NamedScan:: ").append(getRemap(namedScan));
    namedScan
        .getInputs()
        .forEach(
            i -> {
              sb.append(i.accept(this));
            });
    sb.append(" Tables=");
    sb.append(namedScan.getNames());
    sb.append(" Fields=");
    sb.append(namedStruct(namedScan.getInitialSchema()));
    return getOutdent(sb);
  }

  private String namedStruct(NamedStruct struct) {
    var sb = new StringBuilder();

    var names = struct.names();
    var types = fieldList(struct.struct().fields());

    for (var x = 0; x < names.size(); x++) {
      if (x != 0) {
        sb.append(",");
      }
      sb.append(names.get(x)).append("[").append(types.get(x)).append("]");
    }

    return sb.toString();
  }

  @Override
  public String visit(LocalFiles localFiles) throws RuntimeException {
    StringBuilder sb = getIndent().append("LocalFiles:: ");

    for (var i : localFiles.getItems()) {
      sb.append(getContinuationIndentString());
      var fileFormat = "";
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
  public String visit(Project project) throws RuntimeException {
    StringBuilder sb = getIndent().append("Project:: ").append(getRemap(project));

    sb.append(fieldList(project.deriveRecordType().fields()));

    var inputs = project.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(Sort sort) throws RuntimeException {
    StringBuilder sb = getIndent().append("Sort:: ").append(getRemap(sort));
    sort.getSortFields()
        .forEach(
            sf -> {
              var expr = new ExpressionStringify(indent);
              sb.append(sf.expr().accept(expr)).append(" ").append(sf.direction());
            });
    var inputs = sort.getInputs();
    inputs.forEach(
        i -> {
          sb.append(i.accept(this));
        });
    return getOutdent(sb);
  }

  @Override
  public String visit(Cross cross) throws RuntimeException {
    StringBuilder sb = getIndent().append("Cross:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(VirtualTableScan virtualTableScan) throws RuntimeException {
    StringBuilder sb = getIndent().append("VirtualTableScan:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionLeaf extensionLeaf) throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionLeaf:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionSingle extensionSingle) throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionSingle:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionMulti extensionMulti) throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionMulti:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ExtensionTable extensionTable) throws RuntimeException {
    StringBuilder sb = getIndent().append("extensionTable:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(HashJoin hashJoin) throws RuntimeException {
    StringBuilder sb = getIndent().append("hashJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(MergeJoin mergeJoin) throws RuntimeException {
    StringBuilder sb = getIndent().append("mergeJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(NestedLoopJoin nestedLoopJoin) throws RuntimeException {
    StringBuilder sb = getIndent().append("nestedLoopJoin:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(ConsistentPartitionWindow consistentPartitionWindow) throws RuntimeException {
    StringBuilder sb = getIndent().append("consistentPartitionWindow:: ");
    return getOutdent(sb);
  }

  @Override
  public String visit(Expand expand) throws RuntimeException {
    StringBuilder sb = getIndent().append("expand:: ");
    return getOutdent(sb);
  }
}
