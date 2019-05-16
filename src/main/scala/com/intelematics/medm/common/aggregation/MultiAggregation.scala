package com.intelematics.medm.common.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, first, sum, max}

trait MultiAggregation {

  protected def avg_agg(dataFrame: DataFrame, aggRows: Int, groupCol: String, nonAggColumns: List[String]): DataFrame = {
    val aggCols = (dataFrame.columns.toSet - groupCol).map(
      colName => if (nonAggColumns.contains(colName)) {
        first(colName).as(colName)
      } else avg(colName).as(colName)
    ).toList
    dataFrame
      .groupBy(groupCol)
      .agg(aggCols.head, aggCols.tail: _*)
  }

  protected def sum_agg(dataFrame: DataFrame, aggRows: Int, groupCol: String, nonAggColumns: List[String]): DataFrame = {
    val aggCols = (dataFrame.columns.toSet - groupCol).map(
      colName => if (nonAggColumns.contains(colName)) {
        first(colName).as(colName)
      } else sum(colName).as(colName)
    ).toList
    dataFrame
      .groupBy(groupCol)
      .agg(aggCols.head, aggCols.tail: _*)
  }

  protected def max_agg(dataFrame: DataFrame, aggRows: Int, groupCol: String, nonAggColumns: List[String]): DataFrame = {
    val aggCols = (dataFrame.columns.toSet - groupCol).map(
      colName => if (nonAggColumns.contains(colName)) {
        first(colName).as(colName)
      } else max(colName).as(colName)
    ).toList
    dataFrame
      .groupBy(groupCol)
      .agg(aggCols.head, aggCols.tail: _*)
  }
}
