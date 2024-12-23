import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.TableDef
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.CustomAnalyzer
import com.github.jstokes.sparksql_parser.AnalysisResult

class CustomAnalyzerTest extends AnyFunSuite {

  def doAnalysis(sql: String, tables: Seq[TableDef]): AnalysisResult = {
    CustomAnalyzer.builder()
        .withTables(tables.asJava)
        .build()
        .analyze(sql)
  }

  val fooTable = TableDef("foo", StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType)
  )))

  val barTable = TableDef("bar", StructType(Seq(
    StructField("id", LongType),
    StructField("value", DoubleType)
  )))

  test("simple select") {
    val result = doAnalysis("SELECT * FROM foo", Seq(fooTable))
    assert(result.getSchema.map(_.name) === Seq("id", "name"))
    assert(result.getDependencies === Set("foo").asJava)
  }

  test("missing table reference throws") {
    assertThrows[org.apache.spark.sql.AnalysisException] {
      doAnalysis("SELECT * FROM missing", Seq(fooTable))
    }
  }

  test("join analysis") {
    val result = doAnalysis("""
      SELECT f.id, f.name, b.value
      FROM foo f
      JOIN bar b ON f.id = b.id
      """, Seq(fooTable, barTable))
    assert(result.getSchema.map(_.name) === Seq("id", "name", "value"))
    assert(result.getDependencies === Set("foo", "bar").asJava)
  }

  test("subquery analysis") {
    val result = doAnalysis("""
      SELECT * FROM
        (SELECT id FROM foo WHERE id > 0) t
      """, Seq(fooTable))
    assert(result.getSchema.map(_.name) === Seq("id"))
    assert(result.getDependencies === Set("foo").asJava)
  }

  test("empty table context") {
    assertThrows[org.apache.spark.sql.AnalysisException] {
      doAnalysis("SELECT * FROM foo", Seq.empty)
    }
  }
}
