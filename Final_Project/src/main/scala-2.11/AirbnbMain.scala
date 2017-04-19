/**
  * Created by TEJESH on 04/18/2017.
  */
object AirbnbMain {

  def main(args: Array[String]) :Unit ={

    val airbnbRecommendation = new Airbnb
    //    airbnbRecommendation.sc;
    airbnbRecommendation.recommendListing(7,8,5).foreach(println)

  }

}
