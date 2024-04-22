package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def readCsv(path: String): DataFrame = {
    // Lecture de fichiers CSV avec des options
    sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    // Implémentation pour la lecture des tables Hive
    sparkSession.table("nom_de_la_table_Hive")
  }

  // Nouvelle méthode pour la lecture de fichiers Parquet
  def readParquet(path: String): DataFrame = {
    sparkSession.read.parquet(path)
  }
}