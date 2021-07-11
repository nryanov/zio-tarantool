# zio-tarantool
[![GitHub license](https://img.shields.io/github/license/nryanov/zio-tarantool)](https://github.com/nryanov/zio-tarantool/blob/master/LICENSE.txt)
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

The only dependencies are [zio](https://github.com/zio/zio) and [scodec](https://github.com/scodec/scodec).

## Getting started
```scala
import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._
import zio.tarantool.protocol.IteratorCode

/**
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object HelloWorld extends zio.App {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- TarantoolClient.insert("users", User(1, "Name1", 10, Address("street1", 1)))

    user <- TarantoolClient
      .select("users", "primary", 1, 0, IteratorCode.Eq, Tuple1(1L)) // response is Promise[TarantoolError, TarantoolResponse]
      .flatMap(_.await)
      .flatMap(_.head[User])

    _ <- zio.console.putStrLn(s"User: $user")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
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
- `Ping` -- ping
- `Insert` -- insert a tuple
- `Select` -- search for a tuple or a set of tuples in the given space
- `Update` -- update tuple
- `Upsert` -- insert or update tuple
- `Replace` -- replace tuple
- `Delete` -- delete a tuple
- `Eval` -- evaluates and executes the expression in Lua-string, which may be any statement or series of statements
- `Call` -- remote stored-procedure call
- `Execute` -- execute the SQL statement contained in the sql-statement parameter
- `Prepare` -- prepare the SQL statement contained in the sql-statement parameter
- `Refresh` -- force schema cache update

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
To update the schema cache use `TarantoolClient.refresh`.

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
    override def encode(v: TestTuple): Attempt[MpArray] =
      for {
        f1Mp <- Encoder[String].encode(v.f1)
        f2Mp <- Encoder[Int].encode(v.f2)
        f3Mp <- Encoder[Long].encode(v.f3)

        tupleMp <- Encoder[Vector[MessagePack]].encode(Vector(f1Mp, f2Mp, f3Mp))
      } yield tupleMp.asInstanceOf[MpArray]

    override def decode(v: MpArray, idx: Int): Attempt[TestTuple] = {
      val vector = v.value

      val f1Mp = Encoder[String].decode(vector(idx)) // decode the first field
      val f2Mp = Encoder[Int].decode(vector(idx + 1)) // second
      val f3Mp = Encoder[Long].decode(vector(idx + 2)) // third

      for {
        f1 <- f1Mp
        f2 <- f2Mp
        f3 <- f3Mp
      } yield TestTuple(f1, f2, f3)
    }
  }
``` 

## Update operations
There is also an implicit update operation builder for any case class:

```scala
import zio.tarantool.codec.auto._

final case class A(f1: String, f2: Int, f3: Long)

val a = A("1", 2, 3)
val updateOps: Attempt[UpdateOperations] = user.builder.assign(Symbol("f1"), "test").plus(Symbol("f2"), 5).build()
```

Currently, only fields which contains primitive data can be updated. If field does not exist then `Attempt.failure` will be returned.
Also, keep in mind that there is no additional checks for types which are passed into update operations, so there is no error for this:
```scala
user.builder.assign(Symbol("f1"), 12345).build() // tarantool will update field f1 and set value to 12345 
```
But there are checks for operations and types:
```scala
user.builder.minus(Symbol("myField"), "lalala").build() // Attempt.failure because string value cannot be used in numeric operations
```

## Project status
Currently, project is under development, so don't expect full stability and that there are no bugs :)
Also, there may be API changes in the future.   
