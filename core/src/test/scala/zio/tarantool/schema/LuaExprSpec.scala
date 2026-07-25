package zio.tarantool.schema

import _root_.zio.test._
import _root_.zio.test.Assertion._
import zio.tarantool.schema.IndexPart.{ByName, ByPosition}
import zio.tarantool.schema.internal.LuaExpr

object LuaExprSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("LuaExpr")(
      test("escapes string literals") {
        assertTrue(LuaExpr.str("a'b\\c") == "'a\\'b\\\\c'")
      },
      test("renders create_space with options and format") {
        val expr = LuaExpr.createSpace(
          name = "users",
          engine = Some("memtx"),
          temporary = Some(false),
          ifNotExists = Some(true),
          format = List(
            FieldFormat("id", "unsigned"),
            FieldFormat("name", "string", isNullable = true)
          )
        )
        assert(expr)(
          equalTo(
            "box.schema.create_space('users', {engine = 'memtx', temporary = false, if_not_exists = true, " +
              "format = {{name = 'id', type = 'unsigned'}, {name = 'name', type = 'string', is_nullable = true}}})"
          )
        )
      },
      test("renders create_index with parts") {
        val expr = LuaExpr.createIndex(
          space = "users",
          name = "primary",
          indexType = Some("tree"),
          unique = Some(true),
          ifNotExists = Some(true),
          sequence = None,
          parts = List(ByPosition(1, "unsigned"), ByName("name", "string"))
        )
        assert(expr)(
          equalTo(
            "box.space['users']:create_index('primary', {type = 'tree', unique = true, if_not_exists = true, " +
              "parts = {{1, 'unsigned'}, {'name', 'string'}}})"
          )
        )
      },
      test("renders drop and truncate") {
        assertTrue(
          LuaExpr.dropSpace("users", ifExists = true) ==
            "if box.space['users'] then box.space['users']:drop() end",
          LuaExpr.dropSpace("users", ifExists = false) == "box.space['users']:drop()",
          LuaExpr.dropIndex("users", "primary", ifExists = true) ==
            "if box.space['users'] and box.space['users'].index['primary'] then " +
            "box.space['users'].index['primary']:drop() end",
          LuaExpr.truncate("users") == "box.space['users']:truncate()"
        )
      }
    )
}
