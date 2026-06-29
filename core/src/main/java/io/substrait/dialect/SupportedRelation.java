package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A {@code supported_relations} entry. Serializes as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
@JsonDeserialize(using = SupportedRelationDeserializer.class)
@JsonSerialize(using = SupportedRelationSerializer.class)
@Value.Immutable
public abstract class SupportedRelation {
  public abstract RelationKind relation();

  public abstract Optional<Map<String, Object>> metadata();

  /**
   * Join types for {@code JOIN}, {@code HASH_JOIN}, {@code MERGE_JOIN}, {@code NESTED_LOOP_JOIN}.
   */
  public abstract List<JoinType> joinTypes();

  /** Read types for {@code READ}. */
  public abstract List<ReadType> readTypes();

  /** Set operations for {@code SET}. */
  public abstract List<SetOperation> operations();

  /** Write types for {@code WRITE} (serialized as {@code write_types}). */
  public abstract List<WriteType> writeTypes();

  /** Operable object types for {@code DDL} (also serialized as {@code write_types}). */
  public abstract List<DdlWriteType> ddlWriteTypes();

  /** Exchange kinds for {@code EXCHANGE}. */
  public abstract List<ExchangeKind> kinds();

  /** Field types for {@code EXPAND}. */
  public abstract List<ExpandFieldType> fieldTypes();

  /** Supported message type URIs for {@code EXTENSION_SINGLE}/{@code MULTI}/{@code LEAF}. */
  public abstract List<String> messageTypes();

  /**
   * {@code write_types} is a single YAML field shared between {@code WRITE} and {@code DDL}
   * relations, each carrying a different enum. Forbid populating both so serialization stays
   * unambiguous.
   */
  @Value.Check
  protected void checkWriteTypes() {
    if (!writeTypes().isEmpty() && !ddlWriteTypes().isEmpty()) {
      throw new IllegalArgumentException(
          "A supported relation cannot set both writeTypes and ddlWriteTypes; "
              + "they share the write_types field.");
    }
  }

  /**
   * Whether this entry can be written as a bare enum string. Extension relations are never bare:
   * they are absent from the schema's bare-enum list.
   */
  public boolean isBare() {
    switch (relation()) {
      case EXTENSION_SINGLE:
      case EXTENSION_MULTI:
      case EXTENSION_LEAF:
        return false;
      default:
        break;
    }
    return !metadata().isPresent()
        && joinTypes().isEmpty()
        && readTypes().isEmpty()
        && operations().isEmpty()
        && writeTypes().isEmpty()
        && ddlWriteTypes().isEmpty()
        && kinds().isEmpty()
        && fieldTypes().isEmpty()
        && messageTypes().isEmpty();
  }

  public static SupportedRelation of(RelationKind relation) {
    return builder().relation(relation).build();
  }

  public static ImmutableSupportedRelation.Builder builder() {
    return ImmutableSupportedRelation.builder();
  }
}
