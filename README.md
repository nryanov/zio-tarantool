# zio-tarantool
[![GitHub license](https://img.shields.io/github/license/nryanov/zio-tarantool)](https://github.com/nryanov/zio-tarantool/blob/master/LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.nryanov.zio-tarantool/zio-tarantool-core_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.nryanov.zio-tarantool/zio-tarantool-core_2.13)
[![zio-tarantool CI](https://github.com/nryanov/zio-tarantool/actions/workflows/scala.yml/badge.svg?branch=master)](https://github.com/nryanov/zio-tarantool/actions/workflows/scala.yml)

Scala connector for [Tarantool](https://www.tarantool.io/).

## Table of contents
* [Installation](#installation)
* [Getting started](#getting-started)
* [Configuration](#configuration)
* [API reference](#api-reference)
* [Schema](#schema)
* [Codecs](#codecs)
* [Update operations](#update-operations)
* [Project status](#project-status)

## Installation
```sbt
libraryDependencies ++= Seq(
  "com.nryanov.zio-tarantool" %% "zio-tarantool-core" % "[version]" 
)
```

## Getting started
```scala
import zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

/**
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object HelloWorld extends ZIOAppDefault {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run: ZIO[Any, Any, Any] = (for {
    _ <- TarantoolClient.insert.into("users").tuple(User(1, "Name1", 10, Address("street1", 1))).run

    // response is Promise[TarantoolError, TarantoolResponse]
    user <- TarantoolClient.select
      .from("users")
      .index("primary")
      .key(Tuple1(1L))
      .limit(1)
      .run
      .flatMap(_.await)
      .flatMap(_.head[User])

    _ <- Console.printLine(s"User: $user")
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (ZLayer.succeed[Clock](Clock.ClockLive) ++ config) >>> TarantoolClient.live
  }
}
```

More examples can be found in the [examples](examples/) folder.

## Configuration
`TarantoolConfiguration` consists of:
- `ConnectionConfig`
    - `host` -- tarantool instance host.
    - `port` -- tarantool instance port.
    - `connectionTimeoutMillis` -- maximum number of milliseconds to wait before giving up to connect (default: 3000).
    - `retryTimeoutMillis` -- number of milliseconds to wait before retrying if a connection fails (default: 1000).
    - `retries` -- maximum number of times to retry (default: 3).
- `ClientConfig`
    - `requestQueueSize` -- internal queue size for sent requests (default: 64).
    - `useSchemaMetaCache` -- fetched space and index info will be saved in in-memory cache (default: true) .
    - `schemaRequestTimeoutMillis` -- maximum number of milliseconds to wait before giving up to fetch space and index info (default: 10000).
- `AuthInfo` (Without AuthInfo `guest` user will be used)
    - `username` -- username to log into tarantool.
    - `password` -- password to log into tarantool.

In the most cases defaults should be ok, and you can use simplified constructors which require only host, port and optionally auth info:
```scala
TarantoolConfig(host, port) // default user: guest
TarantoolConfig(host, port, AuthInfo(username, password))
```

## API reference
Operations use fluent builders ending in `.run`:
- `ping` -- ping
- `insert.into(...).tuple(...).run` -- insert a tuple
- `select.from(...).index(...).key(...).limit(...).run` -- search for a tuple or a set of tuples
- `update.in(...).index(...).key(...).ops(...).run` -- update tuple
- `upsert.into(...).index(...).ops(...).tuple(...).run` -- insert or update tuple
- `replace.into(...).tuple(...).run` -- replace tuple
- `delete.from(...).index(...).key(...).run` -- delete a tuple
- `eval.expression(...).run` -- evaluate and execute a Lua expression
- `call.function(...).args(...).run` -- remote stored-procedure call
- `execute.sql(...).run` / `execute.statementId(...).run` -- execute an SQL statement
- `prepare.sql(...).run` / `prepare.statementId(...).run` -- prepare an SQL statement
- `refreshMeta` -- force schema cache update

Space and index can be referenced by name (schema cache) or by numeric id. Select defaults: `offset = 0`, `iterator = Eq`.

All operations return `Promise[TarantoolError, TarantoolResponse]`. `TarantoolResponse` has methods for accessing the actual data:
- `resultSet[A]`
- `head[A]`
- `headOption[A]`
- `raw`

Type parameter `A` with implicit `TupleEncoder[A]` is needed to be able to decode raw messagepack into an actual object.

## Schema
If `ClientConfig.useSchemaMetaCache` is set to `true` then space and index metas will be stored in in-memory cache.
This info will be used in cases where space name and index name are passed instead of their ids. 

Schema will be fetched at the beginning after connection is established, but there is an option to force update it.
To update the schema cache use `TarantoolClient.refreshMeta`.

## Codecs
The core encoder type class is `TupleEncoder[A]`. 
For tuples which consists only of a single primitive type it will be derived from `Encoder[A]`.
For case classes it also will be derived automatically whenever special import is added:
```scala
import zio.tarantool.codec.auto._
```

The data will be encoded as a `MpArray`. The nested types will be flattened and encoded as flat `MpArray`.

But you always can create your own `TupleEncoder[_]`:
```scala
final case class TestTuple(f1: String, f2: Int, f3: Long)

  implicit val tupleEncoder: TupleEncoder[TestTuple] = new TupleEncoder[TestTuple] {

    override def decode(v: ArrayValue, idx: Int): TestTuple = {
      val f1Mp = Encoder[String].decode(v.get(idx))
      val f2Mp = Encoder[Int].decode(v.get(idx + 1))
      val f3Mp = Encoder[Long].decode(v.get(idx + 2))
      TestTuple(f1Mp, f2Mp, f3Mp)
    }

    override def encode(v: TestTuple): Vector[Value] = {
      val f1Mp = Encoder[String].encode(v.f1)
      val f2Mp = Encoder[Int].encode(v.f2)
      val f3Mp = Encoder[Long].encode(v.f3)

      Vector(f1Mp, f2Mp, f3Mp)
    }
  }
``` 

## Update operations
**EXPERIMENTAL**

Instead of manual construction of update operation you can generate it from special implicit builder:

```scala
import zio.tarantool.codec.auto._

final case class A(f1: String, f2: Int, f3: Long)

val a = A("1", 2, 3)
val updateOps: Attempt[UpdateOperations] = user.builder.assign("f1", "test").plus("f2", 5).build()
```

Currently, only fields which contains primitive data can be updated. If field does not exist then `Attempt.failure` will be returned.
Also, keep in mind that there is no additional checks for types which are passed into update operations, so there is no error for this:
```scala
user.builder.assign("f1", 12345).build() // tarantool will update field f1 and set value to 12345 
```
But there are checks for operations and types:
```scala
user.builder.minus("myField", "lalala").build() // Attempt.failure because string value cannot be used in numeric operations
```

## Scala / ZIO support
- Scala: **3.3.x** (default), 2.13.x, 2.12.x
- ZIO: **2.1.x**

## Project status
Currently, project is under development, so don't expect full stability and that there are no bugs :)
Also, there may be API changes in the future.   

