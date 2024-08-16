/**
 * @since 1.0.0
 */
import { hasProperty } from "effect/Predicate"
import * as Schema from "effect/Schema"
import { SnowflakeFromString } from "./Snowflake.js"

/**
 * @since 1.0.0
 * @category type ids
 */
export const TypeId: unique symbol = Symbol.for("@effect/cluster/MessageState")

/**
 * @since 1.0.0
 * @category type ids
 */
export type TypeId = typeof TypeId

/**
 * @since 1.0.0
 * @category type ids
 */
export const isMessageState = (u: unknown): u is MessageState => hasProperty(u, TypeId)

/**
 * Represents the state of a message after it has been delivered to an entity
 * for processing.
 *
 * A message can either be in an `Processing` state, indicating that the
 * message has not yet been processed, or in a `Processed` state, indicating
 * that the message has been processed by the entity and all results are
 * available.
 *
 * @since 1.0.0
 * @category models
 */
export type MessageState = Processing | Processed

/**
 * @since 1.0.0
 * @category models
 */
export class Processing
  extends Schema.TaggedClass<Processing>("@effect/cluster/MessageState/Processing")("Processing", {
    lastConsumedReplyId: Schema.OptionFromNullOr(SnowflakeFromString)
  })
{
  /**
   * @since 1.0.0
   */
  readonly [TypeId] = TypeId
}

/**
 * @since 1.0.0
 * @category models
 */
export class Processed extends Schema.TaggedClass<Processed>("@effect/cluster/MessageState/Processed")("Processed", {
  lastConsumedReplyId: Schema.OptionFromNullOr(SnowflakeFromString)
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId] = TypeId
}

/**
 * @since 1.0.0
 * @category schemas
 */
export const MessageState = Schema.Union(Processing, Processed)
