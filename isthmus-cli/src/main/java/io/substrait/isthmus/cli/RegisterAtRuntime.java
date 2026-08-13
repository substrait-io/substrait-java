package io.substrait.isthmus.cli;

import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.ProtocolMessageEnum;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.substrait.extension.SimpleExtension;
import java.lang.annotation.Annotation;
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
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.BuiltInMethod;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.immutables.value.Value;

/**
 * GraalVM {@link Feature} used during native image generation which registers classes for
 * reflection at runtime using {@link RuntimeReflection}.
 */
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
        substrait.registerByParent(GeneratedMessage.class);
        substrait.registerByParent(GeneratedMessage.Builder.class);
        substrait.registerByParent(ProtocolMessageEnum.class);

        // Substrait immutables.
        substrait.registerByAnnotation(Value.Immutable.class);
      }

      // Records
      register(
          SimpleExtension.TypeArgument.class,
          SimpleExtension.EnumArgument.class,
          SimpleExtension.ValueArgument.class);

      register(
          BuiltInMetadata.class,
          SqlValidatorException.class,
          CalciteContextException.class,
          SqlStdOperatorTable.class,
          StandardConvertletTable.class);

      try (PackageScanner calcite = new PackageScanner("org.apache.calcite")) {
        calcite.registerByParent(Metadata.class);
        calcite.registerByParent(MetadataHandler.class);
        calcite.registerByParent(Resources.Element.class);
      }

      register(
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
          RelMdCollation.class);

      register(Resources.class, SqlValidatorException.class);

      // Calcite reports a syntax error by collecting the tokens the grammar expected at that
      // point, which it does by reflectively calling these productions on the parser
      // (SqlAbstractParserImpl.MetadataImpl.initList). Without them the parser cannot build a
      // SqlParseException at all, and every syntax error surfaces as the reflection failure
      // instead of as a message about the SQL.
      registerMethods(
          SqlDdlParserImpl.class, "ReservedFunctionName", "ContextVariable", "NonReservedKeyWord");

      for (BuiltInMethod method : BuiltInMethod.values()) {
        if (method.field != null) {
          RuntimeReflection.register(method.field);
        }
        if (method.constructor != null) {
          RuntimeReflection.register(method.constructor);
        }
        if (method.method != null) {
          RuntimeReflection.register(method.method);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Registers the named no-argument methods of the given class for reflective lookup.
   *
   * @param c the class declaring the methods
   * @param methodNames the names of the public no-argument methods to register
   * @throws NoSuchMethodException if the class does not declare one of the methods, so that the
   *     native image fails to build rather than silently losing the behavior that needs them
   */
  private static void registerMethods(Class<?> c, String... methodNames)
      throws NoSuchMethodException {
    RuntimeReflection.register(c);
    for (String methodName : methodNames) {
      RuntimeReflection.register(c.getMethod(methodName));
    }
  }

  private static void register(Class<?>... classes) {
    for (Class<?> c : classes) {
      RuntimeReflection.register(c);
      RuntimeReflection.register(c.getDeclaredConstructors());
      RuntimeReflection.register(c.getDeclaredFields());
      RuntimeReflection.register(c.getDeclaredMethods());
    }
  }

  private static final class PackageScanner implements AutoCloseable {
    private final ScanResult scan;

    PackageScanner(String... packageNames) {
      scan =
          new ClassGraph()
              .enableAllInfo()
              // GraalVM native-compile erases the classloader classpath
              .overrideClasspath(System.getProperty("isthmus.classpath"))
              .acceptPackages(packageNames)
              .scan();
    }

    void registerByAnnotation(Class<? extends Annotation> annotation) {
      scan.getClassesWithAnnotation(annotation).loadClasses().forEach(this::registerByParent);
    }

    void registerByParent(Class<?> c) {
      register(c);
      getSubTypes(c).loadClasses().forEach(RegisterAtRuntime::register);
    }

    private ClassInfoList getSubTypes(Class<?> c) {
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
