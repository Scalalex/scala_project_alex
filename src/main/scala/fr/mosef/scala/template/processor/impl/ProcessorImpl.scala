package fr.mosef.scala.template.processor.impl
import org.apache.spark.sql.functions._

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame, report: String): DataFrame = {
    report match {
      case "report1" => generateReport1(inputDF)
      case "report2" => generateReport2(inputDF)
      case "report3" => generateReport3(inputDF)
      case _ => throw new IllegalArgumentException("Invalid report specified")
    }
  }

  private def generateReport1(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("group_key").count()
  }

  private def generateReport2(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("group_key").agg(sum("field1").alias("sum_field1"))
  }

  private def generateReport3(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("group_key").agg(avg("field1").alias("avg_field1"))
  }
}
