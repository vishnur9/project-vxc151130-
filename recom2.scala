import java.io.File
import java.io._
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

//function to compute rmse
def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /** Load ratings from file. */
  def loadRatings(path: String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
 
//function to write output to a file 
def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}


//loading the songs and ratings files
val ratings= sc.textFile("users.csv")
val ratings1=ratings.map(line=>line.split(","))
val ratings2=ratings1.map(line=>(line(0).toInt,Rating(line(1).toInt,line(2).toInt,line(3).toDouble)))
val songs=sc.textFile("songs.csv")
val songs1=songs.map(line=>line.split(","))
val songs2=songs1.map(line=>(line(0).toInt,line(1)))
val songs3=songs2.collect().toMap
val numRatings = ratings2.count()
val numUsers = ratings2.map(_._2.user).distinct().count()
val numSongs= ratings2.map(_._2.product).distinct().count()
val numPartitions = 4

//loading the user ratings file
val myRatings = loadRatings("my.csv") 
val myRatingsRDD = sc.parallelize(myRatings, 1)

//splitting into training,test and validation sets for analysing the model
val training1 = ratings2.filter(x => x._1 < 600).values.union(myRatingsRDD).repartition(numPartitions).cache()
val validation = ratings2.filter(x => x._1 >= 600 && x._1 < 800).values.repartition(numPartitions).cache()
val test = ratings2.filter(x => x._1 >= 800).values.cache()
val numTraining = training1.count()
val numValidation = validation.count()
val numTest = test.count()


// train models and evaluate them on the validation set
//setting up most likely parameters
val ranks = List(8, 12)
val lambdas = List(0.1, 10.0)
val numIters = List(10, 20)
var bestModel: Option[MatrixFactorizationModel] = None
var bestValidationRmse = Double.MaxValue
var bestRank = 0
var bestLambda = -1.0
var bestNumIter = -1
for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
  val model = ALS.train(training1, rank, numIter, lambda)
  val validationRmse = computeRmse(model, validation, numValidation)
  println("RMSE (validation) = " + validationRmse + " for the model trained with rank = " + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }


// evaluate the best model on the test set
val testRmse = computeRmse(bestModel.get, test, numTest)




//building the model using the minimum Rmse
val training = ratings2.values.union(myRatingsRDD).repartition(numPartitions).cache()
val model = ALS.train(training,bestRank,bestNumIter,bestLambda )
val myRatedSongIds = myRatings.map(_.product).toSet
val candidates = sc.parallelize(songs3.keys.filter(!myRatedSongIds.contains(_)).toSeq)
val recommendations = model.predict(candidates.map((0, _))).collect().sortBy(- _.rating).take(50)

val recommendations1 = model.predict(candidates.map((0, _))).collect().sortBy(- _.rating)
val r=recommendations1.map(line=> line.product)

//Writing to a file which serves as an input to pig
printToFile(new File("general_recommendations.txt")) { p =>
  r.foreach(p.println)
}

//split into training,test and validation sets
println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

//analysis of the best model
println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda+ ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")




//recommending a general list of songs
var i = 1
    println("Songs recommended for you:")
    
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + songs3(r.product))
      i += 1
    }
    
    



