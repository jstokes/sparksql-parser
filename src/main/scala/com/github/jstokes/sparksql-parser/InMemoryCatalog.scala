package com.github.jstokes.sparksql_parser

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import java.util.concurrent.ConcurrentHashMap
import java.net.URI

class InMemoryCatalog extends ExternalCatalog {
  private val tables = new ConcurrentHashMap[String, CatalogTable]()
  private val defaultDb = CatalogDatabase("default", "", new URI("file:///tmp/default"), Map.empty)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    tables.putIfAbsent(s"${table.database}.${table.identifier.table}", table)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    Option(tables.get(s"$db.$table")).getOrElse(throw new NoSuchTableException(db, table))
  }

  override def tableExists(db: String, table: String): Boolean = {
    val result = tables.containsKey(s"$db.$table")
    result
  }
  override def databaseExists(db: String): Boolean = db == "default"
  override def getDatabase(db: String): CatalogDatabase = defaultDb

  def createDatabase(dbDefinition: org.apache.spark.sql.catalyst.catalog.CatalogDatabase,ignoreIfExists: Boolean): Unit = {}
  def alterTableStats(db: String,table: String,stats: Option[org.apache.spark.sql.catalyst.catalog.CatalogStatistics]): Unit = ???
  def createFunction(db: String,funcDefinition: org.apache.spark.sql.catalyst.catalog.CatalogFunction): Unit = ???
  def createPartitions(db: String,table: String,parts: Seq[org.apache.spark.sql.catalyst.catalog.CatalogTablePartition],ignoreIfExists: Boolean): Unit = ???
  def dropDatabase(db: String,ignoreIfNotExists: Boolean,cascade: Boolean): Unit = ???
  def dropFunction(db: String,funcName: String): Unit = ???
  def dropPartitions(db: String,table: String,parts: Seq[org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec],ignoreIfNotExists: Boolean,purge: Boolean,retainData: Boolean): Unit = ???
  def dropTable(db: String,table: String,ignoreIfNotExists: Boolean,purge: Boolean): Unit = ???
  def functionExists(db: String,funcName: String): Boolean = ???
  def getFunction(db: String,funcName: String): org.apache.spark.sql.catalyst.catalog.CatalogFunction = ???
  def getPartition(db: String,table: String,spec: org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec): org.apache.spark.sql.catalyst.catalog.CatalogTablePartition = ???
  def getPartitionOption(db: String,table: String,spec: org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec): Option[org.apache.spark.sql.catalyst.catalog.CatalogTablePartition] = ???
  def getTablesByName(db: String,tables: Seq[String]): Seq[org.apache.spark.sql.catalyst.catalog.CatalogTable] = ???
  def listDatabases(pattern: String): Seq[String] = ???
  def listDatabases(): Seq[String] = ???
  def listFunctions(db: String,pattern: String): Seq[String] = ???
  def listPartitionNames(db: String,table: String,partialSpec: Option[org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec]): Seq[String] = ???
  def listPartitions(db: String,table: String,partialSpec: Option[org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec]): Seq[org.apache.spark.sql.catalyst.catalog.CatalogTablePartition] = ???
  def listPartitionsByFilter(db: String,table: String,predicates: Seq[org.apache.spark.sql.catalyst.expressions.Expression],defaultTimeZoneId: String): Seq[org.apache.spark.sql.catalyst.catalog.CatalogTablePartition] = ???
  def listTables(db: String,pattern: String): Seq[String] = ???
  def listTables(db: String): Seq[String] = ???
  def listViews(db: String,pattern: String): Seq[String] = ???
  def loadDynamicPartitions(db: String,table: String,loadPath: String,partition: org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec,replace: Boolean,numDP: Int): Unit = ???
  def loadPartition(db: String,table: String,loadPath: String,partition: org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec,isOverwrite: Boolean,inheritTableSpecs: Boolean,isSrcLocal: Boolean): Unit = ???
  def loadTable(db: String,table: String,loadPath: String,isOverwrite: Boolean,isSrcLocal: Boolean): Unit = ???
  def renameFunction(db: String,oldName: String,newName: String): Unit = ???
  def renamePartitions(db: String,table: String,specs: Seq[org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec],newSpecs: Seq[org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec]): Unit = ???
  def renameTable(db: String,oldName: String,newName: String): Unit = ???
  def setCurrentDatabase(db: String): Unit = ???
  def alterDatabase(dbDefinition: org.apache.spark.sql.catalyst.catalog.CatalogDatabase): Unit = ???
  def alterFunction(db: String,funcDefinition: org.apache.spark.sql.catalyst.catalog.CatalogFunction): Unit = ???
  def alterPartitions(db: String,table: String,parts: Seq[org.apache.spark.sql.catalyst.catalog.CatalogTablePartition]): Unit = ???
  def alterTable(tableDefinition: org.apache.spark.sql.catalyst.catalog.CatalogTable): Unit = ???
  def alterTableDataSchema(db: String,table: String,newDataSchema: org.apache.spark.sql.types.StructType): Unit = ???
}
