package io.substrait.isthmus.cli;

import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageLite;
import com.google.protobuf.ProtocolMessageEnum;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.substrait.extension.SimpleExtension;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdLowerBoundCost;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.BuiltInMethod;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.immutables.value.Value;

public final class RegisterAtRuntime implements Feature {
  @Override
  public void beforeAnalysis(BeforeAnalysisAccess access) {
    try {
      // cli picocli
      register(IsthmusEntryPoint.class);

      // Empty class
      register(Empty.class);

      try (PackageScanner substrait = new PackageScanner("io.substrait")) {
        // protobuf items
        substrait.registerByParent(GeneratedMessageV3.class);
        substrait.registerByParent(MessageLite.Builder.class);
        substrait.registerByParent(ProtocolMessageEnum.class);

        // Substrait immutables.
        substrait.registerByAnnotation(Value.Immutable.class);
      }

      // Records
      register(SimpleExtension.TypeArgument.class);
      register(SimpleExtension.EnumArgument.class);
      register(SimpleExtension.ValueArgument.class);

      register(BuiltInMetadata.class);
      register(SqlValidatorException.class);
      register(CalciteContextException.class);
      register(SqlStdOperatorTable.class);
      register(StandardConvertletTable.class);
      try (PackageScanner calcite = new PackageScanner("org.apache.calcite")) {
        calcite.registerByParent(Metadata.class);
        calcite.registerByParent(MetadataHandler.class);
        calcite.registerByParent(Resources.Element.class);
      }

      Arrays.asList(
              RelMdPercentageOriginalRows.class,
              RelMdColumnOrigins.class,
              RelMdExpressionLineage.class,
              RelMdTableReferences.class,
              RelMdNodeTypes.class,
              RelMdRowCount.class,
              RelMdMaxRowCount.class,
              RelMdMinRowCount.class,
              RelMdUniqueKeys.class,
              RelMdColumnUniqueness.class,
              RelMdPopulationSize.class,
              RelMdSize.class,
              RelMdParallelism.class,
              RelMdDistribution.class,
              RelMdLowerBoundCost.class,
              RelMdMemory.class,
              RelMdDistinctRowCount.class,
              RelMdSelectivity.class,
              RelMdExplainVisibility.class,
              RelMdPredicates.class,
              RelMdAllPredicates.class,
              RelMdCollation.class)
          .forEach(RegisterAtRuntime::register);

      RuntimeReflection.register(Resources.class);
      RuntimeReflection.register(SqlValidatorException.class);

      Arrays.stream(BuiltInMethod.values())
          .forEach(
              c -> {
                if (c.field != null) RuntimeReflection.register(c.field);
                if (c.constructor != null) RuntimeReflection.register(c.constructor);
                if (c.method != null) RuntimeReflection.register(c.method);
              });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void register(final Class<?> c) {
    RuntimeReflection.register(c);
    RuntimeReflection.register(c.getDeclaredConstructors());
    RuntimeReflection.register(c.getDeclaredFields());
    RuntimeReflection.register(c.getDeclaredMethods());
    RuntimeReflection.register(c.getConstructors());
    RuntimeReflection.register(c.getFields());
    RuntimeReflection.register(c.getMethods());
  }

  private static final class PackageScanner implements AutoCloseable {
    private final ScanResult scan;

    PackageScanner(final String... packageNames) {
      scan =
          new ClassGraph()
              .enableAllInfo()
              // GraalVM native-compile erases the classloader classpath
              .overrideClasspath(System.getProperty("isthmus.classpath"))
              .acceptPackages(packageNames)
              .scan();
    }

    void registerByAnnotation(final Class<? extends Annotation> annotation) {
      scan.getClassesWithAnnotation(annotation).loadClasses().forEach(this::registerByParent);
    }

    void registerByParent(final Class<?> c) {
      register(c);
      getSubTypes(c).loadClasses().forEach(RegisterAtRuntime::register);
    }

    private ClassInfoList getSubTypes(final Class<?> c) {
      if (c.isInterface()) {
        return scan.getClassesImplementing(c);
      }

      return scan.getSubclasses(c);
    }

    @Override
    public void close() {
      scan.close();
    }
  }
}
