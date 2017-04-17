import java.util.Scanner

import breeze.numerics.sqrt
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by TEJESH on 04/16/17.
  */
class Airbnb {

  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val directory = "src/main/resources"
  val scanner = new Scanner(System.in)
  val numPartitions = 20
  val topTenListings = getRatingFromUser
  val numTraining = getTrainingRating.count()
  val numTest = getTestingRating.count()
  val numValidate = getValidationRating.count()

  def getRatingRDD: RDD[String] = {

    sc.textFile(directory + "/training_with_price.csv")
  }

  def getListingRDD: RDD[String] = {

    sc.textFile(directory + "/name_listings.csv")
  }

  def getRDDOfRating: RDD[(Long, Rating)] = {

    getRatingRDD.map { line => val fields = line.split(",")

      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  def getListingMap: Map[Int, String] = {

    getListingRDD.map { line => val fields = line.split(",")

      (fields(0).toInt, fields(1))
    }.collect().toMap
  }

  def getTopTenListings: List[(Int, String)] = {

    val top20ListingIDs = getRDDOfRating.map { rating => rating._2.product }
      .countByValue()
      .toList
      .sortBy(-_._2)
      .take(10)
      .map { ratingData => ratingData._1 }

    top20ListingIDs.filter(id => getListingMap.contains(id))
      .map { listing_id => (listing_id, getListingMap.getOrElse(listing_id, "No Movie Found")) }
      .sorted
      .take(3)
  }

  def getRatingFromUser: RDD[Rating] = {
    println("1 Successful")
    val listOFRating = getTopTenListings.map { getRating => {

      println(s"Please Enter The preference For Listings ${getRating._2} From 1 to 10")
      Rating(0, getRating._1, scanner.next().toLong)
    }
    }
    sc.parallelize(listOFRating)
  }

  def getTrainingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 < 7)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

  def getValidationRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 >= 7 && data._1 <= 10)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

  def getTestingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 > 6)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

}

object Airbnb extends App {

  val airbnbRecommendation = new Airbnb
  val sc = airbnbRecommendation.sc
  // Load and parse the data
  //val data = sc.textFile("E:/testfiles/training_with_price.csv")
  val ratings = airbnbRecommendation.getRatingRDD.map(_.split(",") match { case Array(user, item, rate, price) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  val listings = airbnbRecommendation.getListingRDD.map { str => val data = str.split(",")
    (data(0), data(1))
  }
    .map { case (listing_id, name) => (listing_id.toInt, name) }

  val myRatingsRDD = airbnbRecommendation.topTenListings
  val training = ratings.filter { case Rating(listing_id, reviewer_id, rating) => (listing_id * reviewer_id) % 10 <= 1 }.persist
  val test = ratings.filter { case Rating(listing_id, reviewer_id, rating) => (listing_id * reviewer_id) % 10 > 1 }.persist


  val model = ALS.train(training.union(myRatingsRDD), 8, 10, 0.01)

  val ListingsSeen = myRatingsRDD.map(x => x.product).collect().toList

  val ListingsNotSeen = listings.filter { case (listing_id, name) => !ListingsSeen.contains(listing_id) }.map(_._1)

  val predictedRates =
    model.predict(test.map { case Rating(user, item, rating) => (user, item) }).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.persist()

  val ratesAndPreds = test.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictedRates)

  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => Math.pow(r1 - r2, 2)/10 }.mean()

  println("Mean Squared Error = " + MSE)

  val RMSE = sqrt(MSE)
  println("Root Mean Squared Error = " +RMSE)

  val recommendedListingsId = model.predict(ListingsNotSeen.map { product =>
    (0, product)
  }).map { case Rating(user, listings, rating) => (listings, rating) }
    .sortBy(x => x._2, ascending = false).take(10).map(x => x._1)

  val recommendListing = airbnbRecommendation.getListingRDD.map { str => val data = str.split(",")
    (data(0).toInt, data(1))
  }.filter { case (listing_id, name) => recommendedListingsId.contains(listing_id) }

  recommendListing.collect().toList.foreach(println)
  airbnbRecommendation.sc.stop()
}