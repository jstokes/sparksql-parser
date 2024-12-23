package com.github.jstokes.sparksql_parser

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

import org.apache.spark.sql.connector.catalog._

import scala.collection.JavaConverters._

class AnalysisResult (plan: LogicalPlan) {
  private val schemaCopy = plan.schema.copy()
  private val depsCopy = new java.util.HashSet[String](
    plan.collect {
      case r: UnresolvedRelation => r.tableName
      case r: DataSourceV2Relation => r.table.name
    }.asJavaCollection)

  def getSchema(): StructType = schemaCopy
  def getDependencies(): java.util.Set[String] = java.util.Collections.unmodifiableSet(depsCopy)
}
