import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.TableDef

import org.apache.spark.sql.catalyst.analysis.CustomSqlAnalyzer.{analyze, extractSchemaAndDeps}

class SqlAnalyzerTest extends AnyFunSuite {
  val fooTable = TableDef("foo", StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType)
  )))

  val barTable = TableDef("bar", StructType(Seq(
    StructField("id", LongType),
    StructField("value", DoubleType)
  )))

  test("simple select") {
    val result = extractSchemaAndDeps(analyze("SELECT * FROM foo", Seq(fooTable)))
    assert(result._1.map(_.name) === Seq("id", "name"))
    assert(result._2 === Set("foo"))
  }

  test("missing table reference throws") {
    assertThrows[org.apache.spark.sql.AnalysisException] {
      analyze("SELECT * FROM missing", Seq(fooTable))
    }
  }

  test("join analysis") {
    val result = extractSchemaAndDeps(analyze("""
      SELECT f.id, f.name, b.value
      FROM foo f
      JOIN bar b ON f.id = b.id
      """, Seq(fooTable, barTable)))
    assert(result._1.map(_.name) === Seq("id", "name", "value"))
    assert(result._2 === Set("foo", "bar"))
  }

  test("subquery analysis") {
    val result = extractSchemaAndDeps(analyze("""
      SELECT * FROM
        (SELECT id FROM foo WHERE id > 0) t
      """, Seq(fooTable)))
    assert(result._1.map(_.name) === Seq("id"))
    assert(result._2 === Set("foo"))
  }

  test("empty table context") {
    assertThrows[org.apache.spark.sql.AnalysisException] {
      analyze("SELECT * FROM foo", Seq.empty)
    }
  }
}
