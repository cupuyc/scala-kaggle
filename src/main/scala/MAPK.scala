import org.apache.spark.sql.{Row, DataFrame}

/**
  * Created by Stan Reshetnyk on 01.06.16.
  */
object MAPK {

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
    val MAPK = predictions.join(actual, "").map {

      case Row(userId: Int, Row(predictedList @ _*), actual: Int) =>
        //println(userId + " " + predictedList + " " + actual)
        avgPrecisionK(Seq(actual), predictedList.asInstanceOf[Seq[Int]], K)

    }
      .reduce(_ + _) / predictions.count

    println("Validation result " + MAPK)
    MAPK
  }

}
