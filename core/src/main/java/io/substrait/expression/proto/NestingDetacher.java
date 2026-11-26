package io.substrait.expression.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.WireFormat;
import io.substrait.proto.Expression;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Version;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Bounds the protobuf nesting depth of a {@code PlanRel} or {@code ExtendedExpression}. The writer
 * first moves expression subtrees into {@code detached_expressions} (spec v0.105.0). If the
 * relation tree is still too deep, it uses {@code detached_rels} as well (spec v0.107.0).
 *
 * <p>Protobuf parsers limit how deeply messages may nest. The writer leaves a container unchanged
 * if it fits. It first tries to detach subtrees below half the limit, then tries shallower
 * subtrees. Each ordinal refers to an entry in the corresponding table in the same container.
 * Existing entries keep their ordinal positions.
 *
 * <p>If nesting inside a literal, type, or other indivisible message prevents the container from
 * fitting the limit, the writer returns the original container.
 */
public final class NestingDetacher {
  /** The default parse recursion limit of protobuf-java, and of the C++ and upb runtimes. */
  public static final int DEFAULT_MAX_DEPTH = 100;

  private static final NestingDetacher DEFAULT = new NestingDetacher(DEFAULT_MAX_DEPTH, false);
  private static final NestingDetacher DISABLED = new NestingDetacher(Integer.MAX_VALUE, false);
  private static final NestingDetacher EVERY_EXPRESSION = new NestingDetacher(0, true);

  /** Nesting depth of a {@code PlanRel} below the {@code Plan} it is parsed as part of. */
  private static final int PLAN_REL_DEPTH = 1;

  /** The message-typed fields of each message type, the only ones that add nesting. */
  private static final Map<Descriptor, List<FieldDescriptor>> MESSAGE_FIELDS =
      new ConcurrentHashMap<>();

  private final int maxDepth;
  private final boolean everyExpression;

  private NestingDetacher(int maxDepth, boolean everyExpression) {
    this.maxDepth = maxDepth;
    this.everyExpression = everyExpression;
  }

  /**
   * Returns a detacher that keeps a plan within the default parse recursion limit of {@value
   * #DEFAULT_MAX_DEPTH}.
   *
   * @return the default detacher
   */
  public static NestingDetacher defaultDetacher() {
    return DEFAULT;
  }

  /**
   * Returns a detacher that leaves relations and expressions inline.
   *
   * @return a detacher that leaves every container unchanged
   */
  public static NestingDetacher disabled() {
    return DISABLED;
  }

  /**
   * Returns a detacher that detaches every expression below the container, however shallow, so that
   * a consumer's handling of detached expressions can be exercised on any plan.
   *
   * @return a detacher that detaches every expression it can
   */
  public static NestingDetacher everyExpression() {
    return EVERY_EXPRESSION;
  }

  /**
   * Returns a detacher for a consumer that parses up to {@code maxDepth} levels of nesting below
   * the root message, the root itself being at depth 0.
   *
   * @param maxDepth the deepest nesting a consumer accepts
   * @return a detacher for that limit
   */
  public static NestingDetacher withMaxDepth(int maxDepth) {
    if (maxDepth < 0) {
      throw new IllegalArgumentException("maxDepth must not be negative: " + maxDepth);
    }
    return new NestingDetacher(maxDepth, false);
  }

  /**
   * Bounds the nesting of a {@code PlanRel} that is parsed as part of a {@code Plan}.
   *
   * @param planRel the relation to bound
   * @return {@code planRel} itself if it fits, otherwise a bounded copy if its subtrees can be
   *     detached
   */
  public PlanRel detach(PlanRel planRel) {
    return bound(planRel, PLAN_REL_DEPTH);
  }

  /**
   * Bounds the nesting of an {@code ExtendedExpression} that is parsed as the root message.
   *
   * @param extendedExpression the extended expression to bound
   * @return {@code extendedExpression} itself if it fits, otherwise a bounded copy if its subtrees
   *     can be detached
   */
  public ExtendedExpression detach(ExtendedExpression extendedExpression) {
    return bound(extendedExpression, 0);
  }

  /**
   * Raises the declared version when the serialized container uses a newer detached table.
   *
   * @param declared the producer's declared version
   * @param expressionCount the number of detached expressions
   * @param relationCount the number of detached relations
   * @return the original version or the earliest version that supports the tables
   */
  public static Version versionForTables(Version declared, int expressionCount, int relationCount) {
    int requiredMinor = relationCount > 0 ? 107 : expressionCount > 0 ? 105 : 0;
    if (declared.getMajorNumber() == 0
        && Integer.compareUnsigned(declared.getMinorNumber(), requiredMinor) < 0) {
      return declared.toBuilder()
          .setMinorNumber(requiredMinor)
          .setPatchNumber(0)
          .clearGitHash()
          .build();
    }
    return declared;
  }

  private <M extends Message> M bound(M container, int containerDepth) {
    int entryDepth = containerDepth + 1;
    if (everyExpression) {
      return new Pass<>(container, entryDepth, entryDepth, false).run();
    }
    if (maxDepth == Integer.MAX_VALUE || fitsWhole(container, containerDepth)) {
      return container;
    }
    // Prefer the older expression-only encoding whenever it can meet the parser limit.
    for (boolean detachRelations : new boolean[] {false, true}) {
      for (int threshold : new int[] {Math.max(maxDepth / 2, entryDepth), entryDepth}) {
        M detached = new Pass<>(container, entryDepth, threshold, detachRelations).run();
        if (fitsWhole(detached, containerDepth)) {
          return detached;
        }
      }
    }
    return container;
  }

  private boolean fitsWhole(Message container, int containerDepth) {
    return maxDepth >= containerDepth && within(container, maxDepth - containerDepth);
  }

  /** One detachment of one container. */
  private final class Pass<M extends Message> {
    private final M container;
    private final Message.Builder output;
    private final int entryDepth;
    private final int threshold;
    private final boolean detachRelations;
    private final FieldDescriptor expressionTable;
    private final FieldDescriptor relationTable;
    private final Map<Message, Integer> heights = new IdentityHashMap<>();

    Pass(M container, int entryDepth, int threshold, boolean detachRelations) {
      this.container = container;
      this.output = container.toBuilder();
      this.entryDepth = entryDepth;
      this.threshold = threshold;
      this.detachRelations = detachRelations;
      Descriptor descriptor = container.getDescriptorForType();
      this.expressionTable = descriptor.findFieldByName("detached_expressions");
      this.relationTable = descriptor.findFieldByName("detached_rels");
    }

    @SuppressWarnings("unchecked")
    M run() {
      for (FieldDescriptor field : messageFields(container)) {
        if (field.equals(expressionTable) || field.equals(relationTable)) {
          // Keep the root of each existing entry at its ordinal. Its children can still be
          // detached.
          for (int i = 0; i < container.getRepeatedFieldCount(field); i++) {
            Message entry = (Message) container.getRepeatedField(field, i);
            output.setRepeatedField(field, i, children(entry, entryDepth));
          }
        } else {
          copyRewritten(container, output, field, entryDepth);
        }
      }
      return (M) output.build();
    }

    private boolean fits(Message message, int depth) {
      return height(message) <= maxDepth - depth;
    }

    /** Returns the message at {@code depth}, with whatever it contains detached as needed. */
    private Message rewrite(Message message, int depth) {
      if (fits(message, depth)) {
        return message;
      }
      if (depth > threshold) {
        if (message instanceof Expression
            && !((Expression) message).hasDetachedExpressionOrdinal()) {
          return Expression.newBuilder()
              .setDetachedExpressionOrdinal(store(message, expressionTable))
              .build();
        }
        if (detachRelations && message instanceof Rel && !((Rel) message).hasDetachedRelOrdinal()) {
          return Rel.newBuilder().setDetachedRelOrdinal(store(message, relationTable)).build();
        }
      }
      return children(message, depth);
    }

    private int store(Message message, FieldDescriptor table) {
      int ordinal = output.getRepeatedFieldCount(table);
      // Reserve the ordinal before rewriting children, which can append entries to the same table.
      output.addRepeatedField(table, message);
      output.setRepeatedField(table, ordinal, children(message, entryDepth));
      return ordinal;
    }

    /** Returns the message at {@code depth} with each child message rewritten. */
    private Message children(Message message, int depth) {
      if (fits(message, depth)) {
        return message;
      }
      Message.Builder builder = message.toBuilder();
      for (FieldDescriptor field : messageFields(message)) {
        copyRewritten(message, builder, field, depth + 1);
      }
      return builder.build();
    }

    private void copyRewritten(
        Message message, Message.Builder builder, FieldDescriptor field, int childDepth) {
      if (field.isRepeated()) {
        for (int i = 0; i < message.getRepeatedFieldCount(field); i++) {
          builder.setRepeatedField(
              field, i, rewrite((Message) message.getRepeatedField(field, i), childDepth));
        }
      } else if (message.hasField(field)) {
        builder.setField(field, rewrite((Message) message.getField(field), childDepth));
      }
    }

    /** Returns how many levels of messages are nested below {@code message}. */
    private int height(Message message) {
      Integer known = heights.get(message);
      if (known != null) {
        return known;
      }
      int height = 0;
      for (FieldDescriptor field : messageFields(message)) {
        if (field.isRepeated()) {
          for (int i = 0; i < message.getRepeatedFieldCount(field); i++) {
            height = Math.max(height, 1 + height((Message) message.getRepeatedField(field, i)));
          }
        } else if (message.hasField(field)) {
          height = Math.max(height, 1 + height((Message) message.getField(field)));
        }
      }
      heights.put(message, height);
      return height;
    }
  }

  /**
   * Returns whether at most {@code levels} levels of messages are nested below {@code message}.
   *
   * <p>Every container goes through this check, so it scans the serialized form, which the
   * generated code writes without reflection, rather than walking the message tree.
   */
  private static boolean within(Message message, int levels) {
    try {
      return within(
          message.getDescriptorForType(),
          CodedInputStream.newInstance(message.toByteArray()),
          levels);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to scan a serialized " + message.getDescriptorForType().getFullName(), e);
    }
  }

  private static boolean within(Descriptor type, CodedInputStream input, int levels)
      throws IOException {
    while (!input.isAtEnd()) {
      int tag = input.readTag();
      if (WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        FieldDescriptor field = type.findFieldByNumber(WireFormat.getTagFieldNumber(tag));
        if (field != null && field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
          if (levels == 0) {
            return false;
          }
          int limit = input.pushLimit(input.readRawVarint32());
          if (!within(field.getMessageType(), input, levels - 1)) {
            return false;
          }
          input.popLimit(limit);
          continue;
        }
      }
      input.skipField(tag);
    }
    return true;
  }

  private static List<FieldDescriptor> messageFields(Message message) {
    return MESSAGE_FIELDS.computeIfAbsent(
        message.getDescriptorForType(),
        descriptor ->
            descriptor.getFields().stream()
                .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.MESSAGE)
                .collect(Collectors.toList()));
  }
}
