package hBase

import javax.inject.Singleton

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class AverageAnalysisOfListing {


  def getAverageAnalysisOfPriceByRoomType(place: String) = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.AveragePriceByRoomType.getBytes
    val homeColumn: Array[Byte] = AveragePriceByRoomType.EntireHomeApt.getBytes
    val sharedColumn: Array[Byte] = AveragePriceByRoomType.SharedRoom.getBytes
    val privateColumn: Array[Byte] = AveragePriceByRoomType.PrivateRoom.getBytes

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val table = connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = table.get(get)

    val homeAveragePrice = Bytes.toDouble(result.getValue(columnFamily, homeColumn))
    val sharedAveragePrice = Bytes.toDouble(result.getValue(columnFamily, sharedColumn))
    val privateAveragePrice = Bytes.toDouble(result.getValue(columnFamily, privateColumn))

    val aggData = Map(AveragePriceByRoomType.SharedRoom -> sharedAveragePrice, AveragePriceByRoomType.PrivateRoom -> privateAveragePrice, AveragePriceByRoomType.EntireHomeApt -> homeAveragePrice)
    connection.close()
    table.close()
    aggData
  }


  def getAverageAnalysisOfPriceByNoOfRooms(place: String) = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.AveragePriceByNoOfRooms.getBytes
    val one: Array[Byte] = AveragePriceByNoOfRooms.one.getBytes
    val two: Array[Byte] = AveragePriceByNoOfRooms.two.getBytes
    val three: Array[Byte] = AveragePriceByNoOfRooms.three.getBytes
    val fourPlus: Array[Byte] = AveragePriceByNoOfRooms.fourPlus.getBytes

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val tabel = connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = tabel.get(get)

    val oneAveragePrice: Array[Byte] = result.getValue(columnFamily, one)
    val twoAveragePrice: Array[Byte] = result.getValue(columnFamily, two)
    val threeAveragePrice: Array[Byte] = result.getValue(columnFamily, three)
    val fourPlusAveragePrice: Array[Byte] = result.getValue(columnFamily, fourPlus)
    val onePrice = Bytes.toDouble(oneAveragePrice)
    val twoPrice = Bytes.toDouble(twoAveragePrice)
    val threePrice = Bytes.toDouble(threeAveragePrice)
    val fourPrice = Bytes.toDouble(fourPlusAveragePrice)
    connection.close()
    tabel.close()
    val aggData = Map(AveragePriceByNoOfRooms.one -> onePrice, AveragePriceByNoOfRooms.two -> twoPrice, AveragePriceByNoOfRooms.three -> threePrice, AveragePriceByNoOfRooms.fourPlus -> fourPrice)
    aggData
  }


}
