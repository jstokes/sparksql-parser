import org.apache.spark.sql.catalyst.analysis.TableDef
import org.apache.spark.sql.catalyst.analysis.CustomSqlAnalyzer
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._


object Main extends App {
  val tables = Seq(
    TableDef("foo", StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType)))))
  val sqlString = "select * from foo"
  val logicalPlan = CustomSqlAnalyzer.analyze(sqlString, tables.asJava)
  println(CustomSqlAnalyzer.extractSchemaAndDeps(logicalPlan))
}
