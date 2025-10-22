package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

public class SubtraitRelVisitorExtensionTest {

  public class Employee {
    public final int DEPT_ID;
    public final String NAME;

    public Employee(int deptId, String name) {
      this.DEPT_ID = deptId;
      this.NAME = name;
    }
  }

  public class DemoSchema {
    public final Employee[] EMPLOYEES = new Employee[0];
  }

  public static class RepeatRel extends SingleRel {

    private final int repeatCount;

    protected RepeatRel(
        final RelOptCluster cluster,
        final RelTraitSet traits,
        final RelNode input,
        final int repeatCount) {
      super(cluster, traits, input);
      this.repeatCount = repeatCount;
    }

    public static RepeatRel create(final RelNode input, final int repeatCount) {
      if (repeatCount <= 0) {
        throw new IllegalArgumentException("Repeat count must be positive");
      }
      return new RepeatRel(input.getCluster(), input.getTraitSet(), input, repeatCount);
    }

    public int getRepeatCount() {
      return repeatCount;
    }

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
      assert inputs.size() == 1;
      return new RepeatRel(getCluster(), traitSet, inputs.get(0), this.repeatCount);
    }
  }

  public static class SubstraitRepeatRel extends SingleInputRel {
    private final Rel input;
    private final int repeatCount;

    public SubstraitRepeatRel(final Rel input, final int repeatCount) {
      this.input = input;
      this.repeatCount = repeatCount;
    }

    @Override
    public Rel getInput() {
      return input;
    }

    @Override
    protected Type.Struct deriveRecordType() {
      return input.getRecordType();
    }

    @Override
    public Optional<Remap> getRemap() {
      return input.getRemap();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return input.getCommonExtension();
    }

    @Override
    public Optional<Hint> getHint() {
      return input.getHint();
    }

    @Override
    public <O, C extends VisitationContext, E extends Exception> O accept(
        final RelVisitor<O, C, E> visitor, final C context) throws E {
      return null;
    }

    public int getRepeatCount() {
      return repeatCount;
    }
  }

  public static class SubstraitRelVisitorCustom extends SubstraitRelVisitor {

    public SubstraitRelVisitorCustom(
        final RelDataTypeFactory typeFactory,
        final SimpleExtension.ExtensionCollection extensions) {
      super(typeFactory, extensions);
    }

    private Rel handleCustomJoin(final RepeatRel repeatRel) {
      final Rel input = apply(repeatRel.getInput());
      return new SubstraitRepeatRel(input, repeatRel.getRepeatCount());
    }

    @Override
    public Rel visitOther(final RelNode other) {
      if (other instanceof RepeatRel) {
        return handleCustomJoin((RepeatRel) other);
      }
      throw new UnsupportedOperationException("Unable to handle node: " + other);
    }

    public static Rel convert(
        final RelNode relNode, final SimpleExtension.ExtensionCollection extensions) {
      final SubstraitRelVisitorCustom visitor =
          new SubstraitRelVisitorCustom(relNode.getCluster().getTypeFactory(), extensions);
      visitor.popFieldAccessDepthMap(relNode);
      return visitor.apply(relNode);
    }
  }

  public static class CustomRelBuilder extends RelBuilder {
    protected CustomRelBuilder(
        final Context context, final RelOptCluster cluster, final RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }

    public static CustomRelBuilder create(FrameworkConfig config) {
      return Frameworks.withPrepare(
          config,
          (cluster, relOptSchema, rootSchema, statement) ->
              new CustomRelBuilder(config.getContext(), cluster, relOptSchema));
    }

    public CustomRelBuilder repeat(final int repeatCount) {
      RelNode input = this.peek();
      RelNode repeatNode = RepeatRel.create(input, repeatCount);
      this.push(repeatNode);
      return this;
    }

    @Override
    public CustomRelBuilder scan(final String... tableNames) {
      super.scan(tableNames);
      return this;
    }

    @Override
    public CustomRelBuilder scan(final Iterable<String> tableNames) {
      super.scan(tableNames);
      return this;
    }

    @Override
    public CustomRelBuilder filter(final RexNode... predicates) {
      super.filter(predicates);
      return this;
    }

    @Override
    public CustomRelBuilder filter(final Iterable<? extends RexNode> predicates) {
      super.filter(predicates);
      return this;
    }

    @Override
    public CustomRelBuilder project(final RexNode... nodes) {
      super.project(nodes);
      return this;
    }

    @Override
    public CustomRelBuilder project(final Iterable<? extends RexNode> nodes) {
      super.project(nodes);
      return this;
    }
  }

  public static boolean findNode(final Rel rel, final Class<? extends Rel> targetClass) {

    if (targetClass.isInstance(rel)) {
      return true;
    }
    final List<Rel> inputs = rel.getInputs();
    if (inputs == null || inputs.isEmpty()) {
      return false;
    }

    for (final Rel input : inputs) {
      if (findNode(input, targetClass)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void test() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus salesSchema = rootSchema.add("SALES", new ReflectiveSchema(new DemoSchema()));

    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(salesSchema)
            .build();
    final CustomRelBuilder builder = CustomRelBuilder.create(config);

    final RelNode plan =
        builder
            .scan("EMPLOYEES")
            .filter(builder.equals(builder.field("DEPT_ID"), builder.literal(10)))
            .repeat(3)
            .project(builder.field("NAME"))
            .build();

    final Rel rel =
        SubstraitRelVisitorCustom.convert(plan, DefaultExtensionCatalog.DEFAULT_COLLECTION);
    assertTrue(
        findNode(rel, SubstraitRepeatRel.class),
        "substrait plan must contain SubstraitRepeatRel relation");
  }
}
