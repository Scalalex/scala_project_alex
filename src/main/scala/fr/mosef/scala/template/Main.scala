package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App with Job {

  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }
  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      print("No input defined")
      sys.exit(1)
    }
  }
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer()
  val src_path = SRC_PATH
  val dst_path = DST_PATH

  val inputDF: DataFrame = {
    if (src_path.endsWith(".csv")) {
      reader.readCsv(src_path)
    } else if (src_path.endsWith(".parquet")) {
      reader.readParquet(src_path)
    } else if (src_path.startsWith("hive://")) {
      throw new UnsupportedOperationException("Reading from Hive is not implemented yet")
    } else {
      throw new IllegalArgumentException("Unsupported file format")
    }
  }

  val report: String = try {
    cliArgs(2) // Utilisation du troisième argument
  } catch {
    case _: java.lang.ArrayIndexOutOfBoundsException => {
      println("No report specified, defaulting to 'report1'")
      "report1"
    }
  }

  val processedDF: DataFrame = process(report)
  writer.write(processedDF, "overwrite", dst_path)

  override def process(report: String): DataFrame = {
    processor.process(inputDF, report)
  }

}
