package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame

trait Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame

  def readCsv(path: String): DataFrame

  def read(): DataFrame

  // Nouvelle méthode pour la lecture de fichiers Parquet
  def readParquet(path: String): DataFrame

}
