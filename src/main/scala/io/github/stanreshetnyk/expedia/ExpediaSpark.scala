import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.linalg.{ Vectors, Vector }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{ SparkConf, SparkContext }

import scala.tools.nsc.io.File

import org.apache.spark.sql.functions._

import org.apache.spark.mllib.feature.PCA

/**
 * From https://www.kaggle.com/c/expedia-hotel-recommendations/forums/t/20488/beating-the-benchmark-in-scala-on-a-spark-cluster
 * Porting from Python to Scala DataFrame with a little of RDD
 */
object ExpediaSpark extends App {

  val csvFormat = "com.databricks.spark.csv"

  val STORAGE = "data/expedia/"
  val RESULT_FILE = STORAGE + "result.csv"
  val TRAIN_FILE = STORAGE + "train.csv"
  val DESTINATIONS_FILE = STORAGE + "destinations.csv"
  val TRAIN_SMALL_FILE = STORAGE + "train-small.csv"
  val TEST_FILE = STORAGE + "test.csv"

  val fg = "s3n://your-bucket-nameDateTime/train/*"

  val HOTEL_CLUSTER = "hotel_cluster"
  val USER_ID = "user_id"

  Logger.getLogger("org").setLevel(Level.WARN)

  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Expedia")
      .set("spark.app.id", "spark")
      .setMaster("local[*]"))

  val sqlContext = new SQLContext(sc)

  start()

  def start() {
    //  val formatter = DateTimeFormat.forPattern("yyyy-mm-dd'T'kk:mm:ss")

    // create smaller data to work with
    if (!File(TRAIN_SMALL_FILE).exists) {
      val partData = sc.textFile(TRAIN_FILE).take(100000)
      saveToFile(TRAIN_SMALL_FILE, partData)
      println("Created small train file")
    } else {
      println("Using existing small train file")
    }

    val (all, train, test) = loadData(TRAIN_SMALL_FILE)

    train.show(5)

    println("Size of train set: " + train.count())
    println("Size of test set: " + test.count())

    println("Top 5 popular hotel clusters:")
    val top5hotelDF = all.groupBy(HOTEL_CLUSTER).count().sort(col("count").desc)
    top5hotelDF.show(5)

    saveToFile("top5hotels.cvs", top5hotelDF.limit(5))

    val top5hotelList = top5hotelDF.select(HOTEL_CLUSTER).take(5).map(_.getInt(0))
    println("Hotels: " + top5hotelList.mkString(","))

    val predictions = test
      .select(USER_ID)
      .withColumn("result", struct(top5hotelList.map(lit(_)): _*))

    val actual = test.select(USER_ID, HOTEL_CLUSTER)

    validate(predictions, actual)

    val destinations = loadDestinations(DESTINATIONS_FILE)
  }

  def saveToFile(fileName: String, rows: Array[_]): Unit = {
    scala.tools.nsc.io.File(fileName).writeAll(rows.mkString("\n"))
  }

  def saveToFile(fileName: String, df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(fileName)
  }

  def loadData(trainFile: String): (DataFrame, DataFrame, DataFrame) = {
    val nullable = true

    val schemaArray = Array(
      StructField("date_time", TimestampType),
      StructField("site_name", IntegerType, nullable),
      StructField("posa_continent", IntegerType, nullable),
      StructField("user_location_country", IntegerType, nullable),
      StructField("user_location_region", IntegerType, nullable),
      StructField("user_location_city", IntegerType, nullable),
      StructField("orig_destination_distance", FloatType, nullable),
      StructField(USER_ID, IntegerType),
      StructField("is_mobile", IntegerType, nullable),
      StructField("is_package", IntegerType, nullable),
      StructField("channel", StringType, nullable),
      StructField("srch_ci", StringType, nullable),
      StructField("srch_co", StringType, nullable),
      StructField("srch_adults_cnt", StringType, nullable),
      StructField("srch_children_cnt", StringType, nullable),
      StructField("srch_rm_cnt", StringType, nullable),
      StructField("srch_destination_id", StringType, nullable),
      StructField("srch_destination_type_id", StringType, nullable),
      StructField("is_booking", StringType, nullable),
      StructField("cnt", StringType, nullable),
      StructField("hotel_continent", StringType, nullable),
      StructField("hotel_country", StringType, nullable),
      StructField("hotel_market", StringType, nullable),
      StructField(HOTEL_CLUSTER, IntegerType))

    val trainSchema = StructType(schemaArray)

    val df = loadDataFrame(trainFile, Some(trainSchema))

      // add year and month as separate columns
      .withColumn("year", year(col("date_time")))
      .withColumn("month", month(col("date_time")))

      // we don't need not booked activity
      .filter(col("is_booking") eqNullSafe 1)

    df.printSchema()

    val trainDF = df.filter(df("year") lt 2014)
    val testDF = df.filter(df("year") eqNullSafe 2014)

    (df, trainDF, testDF)
  }

  /**
    * Load destinations data, and apply PCA to reduce features from 149 to 3
    */
  def loadDestinations(fileName: String): DataFrame = {
    val dest = loadDataFrame(fileName)

    val pca = new PCA(3).fit(
      dest.rdd.map(destRowToArray))

    val projectedRDD: RDD[Row] = dest.map(
      p => {
        val newData = pca.transform(destRowToArray(p)) // transform only d* columns
        Row.fromSeq(p.get(0) +: newData.toArray) // prepend id back to result
      })

    val schemaOfDest = StructType(Array(
      StructField("srch_destination_id", IntegerType, false),
      StructField("pca_d1", DoubleType, false),
      StructField("pca_d2", DoubleType, false),
      StructField("pca_d3", DoubleType, false)))

    val projected = sqlContext.createDataFrame(projectedRDD, schemaOfDest)

    println("Size of projected " + projectedRDD.first().size + " " + projectedRDD.count())
    projected
  }

  def destRowToArray(r: Row): Vector = {
    val v = r.toSeq.tail.toArray.map {
      case d: Double => d
      case _ => 0.0
    }
    Vectors.dense(v)
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

  /**
   * Mean average precision at K.
   * Copied from "Machine Learning with Spark" book
   */
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)

      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }

  def validate(predictions: DataFrame, actual: DataFrame): Double = {
    val K = 5
    val MAPK = predictions.join(actual, USER_ID).map {

      case Row(userId: Int, Row(predictedList @ _*), actual: Int) =>
        //println(userId + " " + predictedList + " " + actual)
        avgPrecisionK(Seq(actual), predictedList.asInstanceOf[Seq[Int]], K)

    }
      .reduce(_ + _) / predictions.count

    println("Validation result " + MAPK)
    MAPK
  }
}