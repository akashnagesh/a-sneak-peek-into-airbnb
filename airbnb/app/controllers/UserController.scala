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
    //val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")

    //val somesd =  maps += mapsforRooms
    // println(maps)
    //maps.toList.foreach(x => println(x))
    val some = Json.toJson(maps)
    Ok(Json.toJson(maps))
  }


  def graph1 = Action {

    //val maps= averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")
    val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")
    //print(mapsforRooms)
    //println(maps)
    mapsforRooms.toList.foreach(x => println(x))
    //val some = Json.toJson(mapsforRooms)
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
