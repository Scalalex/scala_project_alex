package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
import java.util.Properties
import scala.io.Source

class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    val props = loadProperties()
    val format = props.getProperty("format", "csv")
    val separator = props.getProperty("separator", ",")

    format match {
      case "csv" =>
        df.write.option("header", "true").option("sep", separator).mode(mode).csv(path)
      case "hive" =>
        df.write.mode(mode).saveAsTable(path)
      case "parquet" =>
        df.write.mode(mode).parquet(path)
      case _ =>
        throw new IllegalArgumentException("Unsupported format")
    }
  }

  private def loadProperties(): Properties = {
    val props = new Properties()
    val configFile = "application.properties"
    val stream = getClass.getClassLoader.getResourceAsStream(configFile)
    if (stream != null) {
      props.load(stream)
      stream.close()
    }
    props
  }
}
