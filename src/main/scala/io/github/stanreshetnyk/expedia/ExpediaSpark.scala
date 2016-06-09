package io.github.stanreshetnyk

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.expressions.{ WindowSpec, Window }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{ SparkConf, SparkContext }

import scala.io.Source
import scala.tools.nsc.io.File

import org.apache.spark.sql.functions._

import org.apache.spark.storage.StorageLevel

/**
 * From http://nbviewer.jupyter.org/gist/vykhand/1f2484ff14fbbf805234160cf90668b4#as-mentioned-earlier,-you-can-monitor-jobs-on-spark_machine:4040-by-default
 *
 * Porting from Python to Scala DataFrame with a little of RDD
 * Score of submission is 0.49155
 *
 * Leakage Solution description:
 * You can find hotel_clusters for the affected rows by matching rows from the train dataset
 * based on the following columns: user_location_country, user_location_region, user_location_city,
 * hotel_market and orig_destination_distance. However, this will not be 100% accurate
 * because hotels can change cluster assignments (hotels popularity and price have seasonal characteristics).
 */
object ExpediaSpark extends App {

  val IS_TEST_RUN = false

  val csvFormat = "com.databricks.spark.csv"

  val STORAGE = "data/expedia/"
  val RESULT_FILE = STORAGE + "result.csv"
  val TRAIN_FILE = STORAGE + "train.csv"
  val DESTINATIONS_FILE = STORAGE + "destinations.csv"
  val TRAIN_SMALL_FILE = STORAGE + "train-small.csv"
  val TEST_SMALL_FILE = STORAGE + "test-small.csv"
  val TEST_FILE = STORAGE + "test.csv"

  // Columns
  val DATE_TIME = "date_time"
  val HOTEL_CLUSTER = "hotel_cluster"
  val USER_ID = "user_id"
  val SRCH_CI = "srch_ci"
  val SRCH_CO = "srch_co"
  val SRCH_DESTINATION_ID = "srch_destination_id"
  val USER_LOCATION_CITY = "user_location_city"
  val ORIG_DESTINATION_DISTANCE = "orig_destination_distance"
  val HOTEL_COUNTRY = "hotel_country"
  val HOTEL_MARKET = "hotel_market"
  val ID = "id"
  val RN = "rn"
  val IS_BOOKING = "is_booking"
  val CNT = "cnt"
  val RN_ALL = "rn_all"

  Logger.getLogger("org").setLevel(Level.WARN)

  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Expedia")
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

    val (trainFile, testFile) = IS_TEST_RUN match {

      case true =>
        // create smaller data to work with
        if (!File(TRAIN_SMALL_FILE).exists) {
          val partData = sc.textFile(TRAIN_FILE).take(100000)
          saveToFile(TRAIN_SMALL_FILE, partData)
          println("Created small train file")
        } else {
          println("Using existing small train file")
        }

        // create smaller data to work with
        if (!File(TEST_SMALL_FILE).exists) {
          val partData = sc.textFile(TEST_FILE).take(100000)
          saveToFile(TEST_SMALL_FILE, partData)
          println("Created small test file")
        } else {
          println("Using existing small test file")
        }
        (TRAIN_SMALL_FILE, TEST_SMALL_FILE)

      case false =>
        (TRAIN_FILE, TEST_FILE)
    }

    val train = loadData(trainFile)

    // train.show(5)

    //    println("Size of train set: " + train.count())

    // Leakage solution

    val formula1 = col(IS_BOOKING) * 12 + 3
    val formula2 = col(IS_BOOKING) * 5 + 3

    val w1 = Window.partitionBy(USER_LOCATION_CITY, ORIG_DESTINATION_DISTANCE).orderBy(col(CNT).desc)

    val agg_ulc_odd_hc = selectWithRowNumber(train
      .filter(col(ORIG_DESTINATION_DISTANCE) isNotNull)
      .filter(col(USER_LOCATION_CITY) isNotNull)
      .withColumn(ORIG_DESTINATION_DISTANCE, col(ORIG_DESTINATION_DISTANCE) multiply 100000)
      .select(USER_LOCATION_CITY, ORIG_DESTINATION_DISTANCE, HOTEL_CLUSTER, IS_BOOKING)
      .groupBy(col(USER_LOCATION_CITY), col(ORIG_DESTINATION_DISTANCE), col(HOTEL_CLUSTER))
      .count()
      .withColumnRenamed("count", CNT), w1)

    // ---------------------- //

    val cols1 = List(SRCH_DESTINATION_ID, HOTEL_COUNTRY, HOTEL_MARKET).map(col)
    val cols2 = cols1 :+ col(HOTEL_CLUSTER)
    val WB = "wb"
    val SUM_WB = "sum_wb"

    val w2 = Window.partitionBy(cols1: _*).orderBy(col(SUM_WB).desc)
    val agg_best_search_dest_ctry = selectWithRowNumber(train
      .filter(year(col(DATE_TIME)) equalTo 2014)
      .select((cols2 :+ formula1.alias(WB)): _*)
      .groupBy(cols2: _*)
      .sum(WB)
      //      .withColumn(WB, sum(col(WB)))
      //      .withColumnRenamed(WB, SUM_WB)
      .withColumnRenamed("sum(wb)", SUM_WB)
      .orderBy(col(SUM_WB).desc), w2)

    // ---------------------- //

    val w3 = Window.partitionBy(col(SRCH_DESTINATION_ID)).orderBy(col(SUM_WB).desc)
    val agg_best_search_dest_2 = selectWithRowNumber(train
      .select(col(SRCH_DESTINATION_ID), col(HOTEL_CLUSTER), formula1.alias(WB))
      .groupBy(col(SRCH_DESTINATION_ID), col(HOTEL_CLUSTER))
      .sum(WB)
      .withColumnRenamed("sum(wb)", SUM_WB)
      .orderBy(col(SUM_WB).desc), w3)

    // ---------------------- //
    // most popular hotels

    val agg_popular_hotel_cluster = train
      .select(col(HOTEL_CLUSTER), formula2.alias(WB))
      .groupBy(HOTEL_CLUSTER)
      .sum(WB)
      .withColumnRenamed("sum(wb)", SUM_WB)
      .orderBy(col(SUM_WB).desc)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // ---------------------- //
    // broadcasting top5 hotels

    val top5_hotels: List[Int] = agg_popular_hotel_cluster.rdd.take(5).map(_.getInt(0)).toList
    val top5_bc = sc.broadcast(top5_hotels)
    println("Top 5 hotel clusters: " + top5_hotels.mkString(" "))

    // ---------------------- //
    train.unpersist()

    val test = loadData(testFile, false)
      .withColumn(ORIG_DESTINATION_DISTANCE, col(ORIG_DESTINATION_DISTANCE) multiply 100000)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // test.show(5)

    val test_join_1 = test.join(agg_ulc_odd_hc, Seq(USER_LOCATION_CITY, ORIG_DESTINATION_DISTANCE))
      .select(ID, HOTEL_CLUSTER, RN)
      .orderBy(ID, RN)

    val test_join_2 = test.join(agg_best_search_dest_ctry, Seq(SRCH_DESTINATION_ID, HOTEL_COUNTRY, HOTEL_MARKET))
      .select(col(ID), HOTEL_CLUSTER, RN)
      .withColumn(RN, col(RN) * 10)
      .orderBy(ID, RN)

    val test_join_3 = test.join(agg_best_search_dest_2, SRCH_DESTINATION_ID)
      .select(ID, HOTEL_CLUSTER, RN)
      .withColumn(RN, col(RN) * 100)
      .orderBy(ID, RN)

    val not_matched_ids = test.select(ID)
      .except(test_join_1.select(ID).distinct())
      .except(test_join_2.select(ID).distinct())
      .except(test_join_3.select(ID).distinct())

    val test_remainder = not_matched_ids.join(agg_popular_hotel_cluster.limit(5)).selectExpr(ID, HOTEL_CLUSTER, "999 as rn")

    // ---------------------- //

    val w4 = Window.partitionBy(ID).orderBy(RN)
    val test_union = selectWithRowNumber(test_join_1
      .unionAll(test_join_2)
      .unionAll(test_join_3)
      .unionAll(test_remainder), w4, RN_ALL, false)
      .orderBy(ID, RN_ALL)

    // test_union.show(5)

    val submission = test_union
      .orderBy(ID, RN_ALL)
      .rdd
      .map(x => {
        (x.getInt(0), List(x.getInt(1)))
      })
      .reduceByKey((a, b) => a ++ b)
      .mapValues(x => (x ++ top5_bc.value).take(5))
      .mapValues(x => x.take(5).mkString(" "))
      .map(x => Row(x._1, x._2))

    val submissionSchema = new StructType(Array(
      StructField(ID, IntegerType),
      StructField(HOTEL_CLUSTER, StringType)))

    val submissionDF = sqlContext
      .createDataFrame(submission, submissionSchema)
      .orderBy(ID)
      .repartition(1)
    // submissionDF.show(5)

    saveToDF(RESULT_FILE, submissionDF)
  }

  def selectWithRowNumber(df: DataFrame, ws: WindowSpec, alias: String = RN, cache: Boolean = true): DataFrame = {
    val res1 = df.select((df.columns.map(col) :+ row_number.over(ws).alias(alias)): _*)
      .filter(col(alias) leq 5)

    val res = if (cache) res1.persist(StorageLevel.MEMORY_AND_DISK) else res1

    // res.show(5)
    res
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

    // Without bellow code block - calculation failed with ArgumentException in java.sql.Date.valueOf()
    Source.fromFile(trainFile).getLines.flatMap(line => {
      val arr = line.split(",")
      val index: Int = (if (isTraining) 0 else 1)
      if (arr(index) == null) Some("Problem with date: " + line) else None
    }).foreach(println)

    val schemaArray = Array(
      StructField(DATE_TIME, TimestampType, NOT_NULL),
      StructField("site_name", IntegerType, nullable),
      StructField("posa_continent", IntegerType, nullable),
      StructField("user_location_country", IntegerType, nullable),
      StructField("user_location_region", IntegerType, nullable),
      StructField(USER_LOCATION_CITY, IntegerType, nullable),
      StructField(ORIG_DESTINATION_DISTANCE, FloatType, nullable),
      StructField(USER_ID, IntegerType, NOT_NULL),
      StructField("is_mobile", IntegerType, nullable),
      StructField("is_package", IntegerType, nullable),
      StructField("channel", IntegerType, nullable),
      StructField(SRCH_CI, StringType, nullable),
      StructField(SRCH_CO, StringType, nullable),
      StructField("srch_adults_cnt", IntegerType, nullable),
      StructField("srch_children_cnt", IntegerType, nullable),
      StructField("srch_rm_cnt", IntegerType, nullable),
      StructField(SRCH_DESTINATION_ID, StringType, nullable),
      StructField("srch_destination_type_id", StringType, nullable),
      StructField(IS_BOOKING, IntegerType, nullable),
      StructField("cnt", IntegerType, nullable),
      StructField("hotel_continent", IntegerType, nullable),
      StructField(HOTEL_COUNTRY, IntegerType, nullable),
      StructField(HOTEL_MARKET, IntegerType, nullable))

    val schemaArrayExtra = isTraining match {
      case true => schemaArray :+ StructField(HOTEL_CLUSTER, IntegerType, NOT_NULL)
      case false => StructField(ID, IntegerType, NOT_NULL) +: schemaArray
    }

    val origDF = loadDataFrame(trainFile, Some(StructType(schemaArrayExtra)))

    origDF.printSchema()
    //    origDF.select(avg(col(DATE_TIME))).show(5)

    origDF
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