package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
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

class SubtraitRelVisitorExtensionTest {

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
      if (inputs.size() != 1) {
        throw new IllegalArgumentException(
            "RepeatRel requires exactly one input, but got " + inputs.size());
      }
      return new RepeatRel(getCluster(), traitSet, inputs.get(0), this.repeatCount);
    }
  }

  /**
   * A custom (non-Immutables) {@link Rel} wrapping another relation, standing in for the kind of
   * relation an integration adds for a dialect-specific operator.
   *
   * <p>It owns its own {@code RelCommon} data rather than delegating it: the record type is derived
   * from the input, but an alias, row-count stats, an emit mapping or an anchor describe
   * <em>this</em> relation, so forwarding a {@code withXxx} call to the input would overwrite
   * whatever the input carried in its own right — and for the anchor it would leave two relations
   * reporting the same id, which {@link Rel#getRelAnchor()} requires to be unique across a plan.
   */
  public static class SubstraitRepeatRel extends SingleInputRel {
    private final Rel input;
    private final int repeatCount;
    private final Optional<Remap> remap;
    private final Optional<Hint> hint;
    private final Optional<AdvancedExtension> commonExtension;
    private final Optional<Integer> relAnchor;

    public SubstraitRepeatRel(final Rel input, final int repeatCount) {
      this(
          input,
          repeatCount,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    private SubstraitRepeatRel(
        final Rel input,
        final int repeatCount,
        final Optional<Remap> remap,
        final Optional<Hint> hint,
        final Optional<AdvancedExtension> commonExtension,
        final Optional<Integer> relAnchor) {
      this.input = input;
      this.repeatCount = repeatCount;
      this.remap = remap;
      this.hint = hint;
      this.commonExtension = commonExtension;
      this.relAnchor = relAnchor;
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
      // Note that deriveRecordType() above already returns the input's *emitted* type, so
      // inheriting
      // the input's mapping would make AbstractRel.getRecordType() apply it a second time.
      return remap;
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return commonExtension;
    }

    @Override
    public Optional<Hint> getHint() {
      return hint;
    }

    @Override
    public Optional<Integer> getRelAnchor() {
      return relAnchor;
    }

    @Override
    public Rel withRelAnchor(final int relAnchor) {
      // Overriding this — rather than inheriting Rel's throwing default — is what lets this custom
      // Rel be the binding point of an id-based outer reference.
      return withRelAnchor(Optional.of(relAnchor));
    }

    @Override
    public Rel withRelAnchor(final Optional<Integer> relAnchor) {
      return new SubstraitRepeatRel(input, repeatCount, remap, hint, commonExtension, relAnchor);
    }

    @Override
    public Rel withRemap(final Optional<? extends Remap> remap) {
      return new SubstraitRepeatRel(
          input, repeatCount, remap.map(Remap.class::cast), hint, commonExtension, relAnchor);
    }

    @Override
    public Rel withCommonExtension(final Optional<? extends AdvancedExtension> commonExtension) {
      return new SubstraitRepeatRel(
          input,
          repeatCount,
          remap,
          hint,
          commonExtension.map(AdvancedExtension.class::cast),
          relAnchor);
    }

    @Override
    public Rel withHint(final Optional<? extends Hint> hint) {
      return new SubstraitRepeatRel(
          input, repeatCount, remap, hint.map(Hint.class::cast), commonExtension, relAnchor);
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
      visitor.resolveOuterReferences(relNode);
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

  @Test
  void customRelSupportsRelAnchor() {
    // A custom (non-Immutables) Rel completes the Rel contract by overriding withRelAnchor, so
    // SubstraitRelVisitor#apply can stamp an id-based outer-reference anchor on it rather than
    // hitting Rel's throwing withRelAnchor default.
    final SubstraitBuilder sb = new SubstraitBuilder();
    final Rel scan =
        sb.namedScan(List.of("t"), List.of("a"), List.of(TypeCreator.REQUIRED.I64))
            .withRelAnchor(4);
    final SubstraitRepeatRel repeat = new SubstraitRepeatRel(scan, 3);

    final Rel anchored = repeat.withRelAnchor(7);
    assertTrue(anchored instanceof SubstraitRepeatRel);
    assertEquals(3, ((SubstraitRepeatRel) anchored).getRepeatCount());
    assertEquals(7, anchored.getRelAnchor().orElseThrow(AssertionError::new));

    // The input keeps its own anchor: an anchor identifies a single relation, so pushing this one
    // down would leave two of them reporting 7.
    assertEquals(
        4,
        ((SubstraitRepeatRel) anchored).getInput().getRelAnchor().orElseThrow(AssertionError::new));

    // Clearing the anchor works too.
    assertFalse(anchored.withRelAnchor(Optional.empty()).getRelAnchor().isPresent());
  }

  @Test
  void customRelSupportsTheRestOfTheRelCommonContract() {
    // The remaining RelCommon copy methods are also overridden, so a custom Rel can be handed to
    // code that stamps an emit mapping, a hint or a common extension onto an arbitrary relation
    // without hitting Rel's throwing defaults. Each is held on the wrapper, so an input carrying
    // RelCommon data of its own keeps it.
    final SubstraitBuilder sb = new SubstraitBuilder();
    final Hint inputHint = Hint.builder().alias("input_alias").build();
    final Rel scan =
        sb.namedScan(
                List.of("t"),
                List.of("a", "b"),
                List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING))
            .withHint(Optional.of(inputHint));
    final SubstraitRepeatRel repeat = new SubstraitRepeatRel(scan, 3);

    // A reordering mapping, so that applying it twice would be observable in the record type.
    final Rel remapped = repeat.withRemap(Optional.of(Rel.Remap.of(List.of(1, 0))));
    assertTrue(remapped instanceof SubstraitRepeatRel);
    assertEquals(List.of(1, 0), remapped.getRemap().orElseThrow(AssertionError::new).indices());
    assertEquals(
        List.of(TypeCreator.REQUIRED.STRING, TypeCreator.REQUIRED.I64),
        remapped.getRecordType().fields());

    final Hint hint = Hint.builder().alias("an_alias").build();
    final Rel hinted = repeat.withHint(Optional.of(hint));
    assertEquals(Optional.of(hint), hinted.getHint());
    assertEquals(Optional.of(inputHint), ((SubstraitRepeatRel) hinted).getInput().getHint());

    final AdvancedExtension extension = AdvancedExtension.builder().build();
    final Rel extended = repeat.withCommonExtension(Optional.of(extension));
    assertEquals(Optional.of(extension), extended.getCommonExtension());
    assertFalse(((SubstraitRepeatRel) extended).getInput().getCommonExtension().isPresent());
  }
}
