package zio.tarantool.protocol.annotation

import scala.annotation.StaticAnnotation

/** Fields annotated as PrimaryKey should be skipped in update operations */
final case class PrimaryKey() extends StaticAnnotation
