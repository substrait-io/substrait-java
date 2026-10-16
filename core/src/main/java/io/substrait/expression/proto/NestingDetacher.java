package io.substrait.expression.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.WireFormat;
import io.substrait.proto.Expression.RexTypeCase;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.PlanRel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Bounds the protobuf nesting depth of a {@code PlanRel} or {@code ExtendedExpression} by moving
 * expression subtrees into its {@code detached_expressions} and referring to them by {@code
 * Expression.detached_expression_ordinal} (spec v0.105.0).
 *
 * <p>Protobuf parsers limit how deeply messages may nest; protobuf-java, for one, rejects a message
 * nested more than {@value #DEFAULT_MAX_DEPTH} levels below the root it parses. A container already
 * within the limit is returned unchanged. Otherwise, an expression that does not fit at its
 * position is detached once it lies deeper than half the limit, and its subtree is processed again
 * from its new position directly below the container. If that leaves the container too deep, for
 * instance because the deep part is a chain of relations below a shallow subquery, detachment is
 * retried with every expression below the container eligible. Ordinals are assigned in the order
 * the subtrees are detached, so a detached entry refers only to entries after it.
 *
 * <p>Detachment shortens expression nesting only, not nesting within a chain of relations, a
 * literal or type, or a {@code google.protobuf.Any} payload. A container that still exceeds the
 * limit after both attempts is returned unchanged, so that it does not need a consumer that reads
 * detached expressions without gaining one that parses it.
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
   * Returns a detacher that never detaches an expression.
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
   * Detaches expressions of a {@code PlanRel} that is parsed as part of a {@code Plan}.
   *
   * @param planRel the relation to bound
   * @return {@code planRel} itself if it fits, otherwise a copy with detached expressions
   */
  public PlanRel detach(PlanRel planRel) {
    return bound(planRel, PLAN_REL_DEPTH, planRel.getDetachedExpressionsList());
  }

  /**
   * Detaches expressions of an {@code ExtendedExpression} that is parsed as the root message.
   *
   * @param extendedExpression the extended expression to bound
   * @return {@code extendedExpression} itself if it fits, otherwise a copy with detached
   *     expressions
   */
  public ExtendedExpression detach(ExtendedExpression extendedExpression) {
    return bound(extendedExpression, 0, extendedExpression.getDetachedExpressionsList());
  }

  private <M extends Message> M bound(
      M container, int containerDepth, List<io.substrait.proto.Expression> existing) {
    int entryDepth = containerDepth + 1;
    if (everyExpression) {
      return new Pass(containerDepth, entryDepth, existing).run(container);
    }
    if (maxDepth == Integer.MAX_VALUE || fitsWhole(container, containerDepth)) {
      return container;
    }
    for (int threshold : new int[] {Math.max(maxDepth / 2, entryDepth), entryDepth}) {
      M detached = new Pass(containerDepth, threshold, existing).run(container);
      if (fitsWhole(detached, containerDepth)) {
        return detached;
      }
    }
    return container;
  }

  private boolean fitsWhole(Message container, int containerDepth) {
    return maxDepth >= containerDepth && within(container, maxDepth - containerDepth);
  }

  /** One detachment of one container. */
  private final class Pass {
    private final int containerDepth;
    private final int threshold;
    private final int existingEntries;
    private final List<io.substrait.proto.Expression> entries;
    private final Map<Message, Integer> heights = new IdentityHashMap<>();

    /**
     * @param threshold the depth an expression must lie below to be detached
     */
    Pass(int containerDepth, int threshold, List<io.substrait.proto.Expression> existing) {
      this.containerDepth = containerDepth;
      this.threshold = threshold;
      this.existingEntries = existing.size();
      this.entries = new ArrayList<>(existing);
    }

    @SuppressWarnings("unchecked")
    <M extends Message> M run(M container) {
      FieldDescriptor detachedField =
          container.getDescriptorForType().findFieldByName("detached_expressions");
      Message.Builder builder = container.toBuilder().clearField(detachedField);
      // An existing entry is not moved, so the ordinals referring to it keep their target.
      for (int i = 0; i < existingEntries; i++) {
        entries.set(i, (io.substrait.proto.Expression) children(entries.get(i), entryDepth()));
      }
      for (FieldDescriptor field : messageFields(container)) {
        if (!field.equals(detachedField)) {
          copyRewritten(container, builder, field, containerDepth + 1);
        }
      }
      for (io.substrait.proto.Expression entry : entries) {
        builder.addRepeatedField(detachedField, entry);
      }
      return (M) builder.build();
    }

    private int entryDepth() {
      return containerDepth + 1;
    }

    private boolean fits(Message message, int depth) {
      return height(message) <= maxDepth - depth;
    }

    /** Returns the message at {@code depth}, with whatever it contains detached as needed. */
    private Message rewrite(Message message, int depth) {
      if (fits(message, depth)) {
        return message;
      }
      if (message instanceof io.substrait.proto.Expression
          && depth > threshold
          && ((io.substrait.proto.Expression) message).getRexTypeCase()
              != RexTypeCase.DETACHED_EXPRESSION_ORDINAL) {
        int ordinal = entries.size();
        entries.add(null);
        entries.set(ordinal, (io.substrait.proto.Expression) children(message, entryDepth()));
        return io.substrait.proto.Expression.newBuilder()
            .setDetachedExpressionOrdinal(ordinal)
            .build();
      }
      return children(message, depth);
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
