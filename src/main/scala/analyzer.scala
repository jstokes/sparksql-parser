package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

import java.net.URI

case class TableDef private (name: String, schema: StructType)

object TableDef {
  def apply(name: String, schema: StructType): TableDef = new TableDef(name, schema)
}

class SimpleTable(val schema: StructType, val tableName: String) extends Table {
  override def name(): String = tableName

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.Collections.emptySet()
}

class V2SessionCatalog(val v1Catalog: SessionCatalog) extends TableCatalog with CatalogPlugin {
  override def name(): String = "spark_catalog"
  def renameTable(_1: Identifier, _2: Identifier): Unit = ???
  def alterTable(_1: Identifier, _2: TableChange*): Table = ???
  def createTable(_1: Identifier, _2: StructType, _3: Array[Transform], _4: java.util.Map[String,String]): Table = ???
  def dropTable(_1: Identifier): Boolean = ???

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    v1Catalog.listTables(namespace(0)).map(t =>
      Identifier.of(Array(t.database.getOrElse("default")), t.table)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val v1Table = v1Catalog.getTableMetadata(
      TableIdentifier(ident.name(), Some(ident.namespace()(0)))
    )
    new SimpleTable(v1Table.schema, ident.name())
  }
}

object CustomSqlAnalyzer {

  private def createCatalogManager(tables: java.util.List[TableDef]) = {
    val v1Catalog = new SessionCatalog(
      new InMemoryCatalog,
      FunctionRegistry.builtin,
      TableFunctionRegistry.builtin
    )

    v1Catalog.createDatabase(
      CatalogDatabase(
        "default",
        "Default database",
        new URI("file:///tmp/default"),
        Map.empty
      ),
      ignoreIfExists = true
    )

    tables.asScala.foreach { table =>
      val metadata = new CatalogTable(
        identifier = TableIdentifier(table.name, Some("default")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = table.schema,
        provider = None
      )
      v1Catalog.createTable(metadata, ignoreIfExists = true)
    }

    val v2SessionCatalog = new V2SessionCatalog(v1Catalog)
    new CatalogManager(v2SessionCatalog, v1Catalog)
  }

  def extractSchemaAndDeps(plan: LogicalPlan) = {
    val deps = plan.collect {
      case r: UnresolvedRelation => r.tableName
      case r: DataSourceV2Relation => r.table.name
    }.toSet

    Tuple2(plan.schema, deps)
  }

  def analyze(sql: String, tables: java.util.List[TableDef]): LogicalPlan = {
    val catalogManager = createCatalogManager(tables)
    val analyzer = new Analyzer(catalogManager) {
      override def resolver: Resolver = caseInsensitiveResolution
    }

    val parser = new SparkSqlParser()
    val logicalPlan = parser.parsePlan(sql)
    analyzer.executeAndCheck(logicalPlan, new QueryPlanningTracker())
  }
}
