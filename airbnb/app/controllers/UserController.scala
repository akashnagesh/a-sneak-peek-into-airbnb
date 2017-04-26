package controllers

import javax.inject.{Inject, Singleton}

import hBase.AverageAnalysisOfListing
import play.api.libs.json.Json
import play.api.mvc._

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class UserController @Inject()(averageAnalysisOfListing: AverageAnalysisOfListing) extends Controller {


  def graph = Action {
    val maps = averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")
    Ok(Json.toJson(maps))
  }


  def graph1 = Action {
    val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")
    Ok(Json.toJson(mapsforRooms))
  }

  def getAllGraphs = Action {
    println("getting all the graphs")
    val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")
    val maps = averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")

    val mapsOfMaps = Map("AnalysisOnNumberOfRooms" -> mapsforRooms, "AnalysisOnTypesOfRooms" -> maps)
    //mapsforRooms.toList.foreach(x => println(x))
    Ok(Json.toJson(mapsOfMaps))
  }


}
