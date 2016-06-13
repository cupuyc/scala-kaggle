package io.github.stanreshetnyk.avito

import io.github.stanreshetnyk.common.DownloadDataHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.tools.nsc.io.File

/**
 * Avito Duplicate Ads Detection
 * Porting from Python https://www.kaggle.com/brandao/avito-duplicate-ads-detection/test-ad-1/code
 *
 */
object AvitoSpark extends App {

  val IS_TEST_RUN = true

  val csvFormat = "com.databricks.spark.csv"

  val STORAGE = "data/avito/"
  val LOCATION = STORAGE + "Location.csv"
  var ITEM_PAIRS_TEST = STORAGE + "ItemPairs_test.csv"
  var ITEM_PAIRS_TRAIN = STORAGE + "ItemPairs_train.csv"

  var ITEM_INFO_TEST = STORAGE + "ItemInfo_test.csv"
  var ITEM_INFO_TRAIN = STORAGE + "ItemInfo_train.csv"
  val RESULT_FILE = STORAGE + "result.csv"

  // Columns


  Logger.getLogger("org").setLevel(Level.WARN)

//  DownloadDataHelper.download("https://www.kaggle.com/c/avito-duplicate-ads-detection/download/",
//    STORAGE,
//    List("Location.csv.zip", "ItemPairs_test.csv.zip", "ItemPairs_train.csv.zip",
//      "ItemInfo_test.csv.zip", "ItemInfo_train.csv.zip")
//  )

  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Avito")
      //      .set("spark.app.id", "spark.expedia")
      .set("spark.executor.memory", "12g")
      .set("spark.driver.memory", "4g")
//      .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.dir", "spark-logs")
      //.setMaster("spark://localhost:7077")
      .setMaster("local[*]")
      )

  val sqlContext = new HiveContext(sc)

  start()

  implicit def str2Column(x: String) = col(x)

  def start() {

    // locationID,regionID,
    val location = loadDataFrame(LOCATION)
    val itemPairsTest = loadDataFrame(ITEM_PAIRS_TEST)
    val itemPairsTrain = loadDataFrame(ITEM_PAIRS_TRAIN)
    val itemInfoTest = loadDataFrame(ITEM_PAIRS_TEST)
    val itemInfoTrain = loadDataFrame(ITEM_INFO_TRAIN)



    saveToDF(RESULT_FILE, location)
  }

  def saveToFile(fileName: String, rows: Array[_]): Unit = {
    scala.tools.nsc.io.File(fileName).writeAll(rows.mkString("\n"))
  }

  def saveToDF(fileName: String, df: DataFrame) = {
    df.write
      .format(csvFormat)
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(fileName)
  }

  def saveToFile(fileName: String, df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(fileName)
  }

  def loadData(trainFile: String, isTraining: Boolean = true): DataFrame = {
    val nullable = true
    val NOT_NULL = false

    // locationID,regionID,
    val location = loadDataFrame(LOCATION)
    val itemPairsTest = loadDataFrame(ITEM_PAIRS_TEST)
    val itemPairsTrain = loadDataFrame(ITEM_PAIRS_TRAIN)
    val itemInfoTest = loadDataFrame(ITEM_PAIRS_TEST)
    val itemInfoTrain = loadDataFrame(ITEM_INFO_TRAIN)

//    itemInfoTest <- data.table(itemInfoTest)
//    itemInfoTrain <- data.table(itemInfoTrain)


    location
  }

  private def loadDataFrame(trainFile: String, trainSchema: Option[StructType] = None): DataFrame = {
    val d = sqlContext.read
      .format(csvFormat)
      .option("header", "true")
    (trainSchema match {
      case Some(schema) => d.schema(schema)
      case None => d.option("inferSchema", "true")
    }).load(trainFile)
  }
}