package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, SessionCatalog}
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._
import com.github.jstokes.sparksql_parser.AnalysisResult
import com.github.jstokes.sparksql_parser.InMemoryCatalog


import java.net.URI

case class TableDef private (name: String, schema: StructType)

private class SimpleTable(val schema: StructType, val tableName: String) extends Table {
  override def name(): String = tableName

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.Collections.emptySet()
}

private class CustomCatalog(val v1Catalog: SessionCatalog) extends TableCatalog with CatalogPlugin {
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


class CustomAnalyzer private (tables: java.util.List[TableDef]) {

  private val catalog = new InMemoryCatalog()
  private val sessionCatalog = {
    val sc = new SessionCatalog(
      catalog,
      FunctionRegistry.builtin,
      TableFunctionRegistry.builtin)
    sc.createDatabase(catalog.getDatabase("default"), ignoreIfExists = true)
    tables.forEach { table =>
      sc.createTable(
        CatalogTable(
          identifier = TableIdentifier(table.name, Some("default")),
          tableType = CatalogTableType.MANAGED,
          storage = CatalogStorageFormat.empty,
          schema = table.schema
        ),
        ignoreIfExists = true
      )
    }
    sc
  }

  private val catalogManager = new CatalogManager(
    new CustomCatalog(sessionCatalog),
    sessionCatalog)

  private val analyzer = new Analyzer(catalogManager) {
    override def resolver: Resolver = caseInsensitiveResolution
  }

  private val parser = new SparkSqlParser()

  def analyze(sql: String): AnalysisResult = {
    val plan = parser.parsePlan(sql)
    val analyzed = analyzer.executeAndCheck(plan, new QueryPlanningTracker())
    new AnalysisResult(analyzed)
  }
}

object CustomAnalyzer {
  class Builder {
    private val tables = new java.util.ArrayList[TableDef]()

    def withTable(name: String, schema: StructType): Builder = {
      tables.add(TableDef(name, schema))
      this
    }

    def withTables(tables: java.util.List[TableDef]): Builder = {
      this.tables.addAll(tables)
      this
    }

    def build(): CustomAnalyzer = new CustomAnalyzer(tables)
  }

  def builder(): Builder = new Builder()
}
