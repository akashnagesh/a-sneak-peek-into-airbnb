package hBase

import javax.inject.Singleton

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class hBase {


  import org.apache.hadoop.hbase.client.ConnectionFactory


  //Return Connection pool for an HBase instance
  def getConnect ={
    val conf: Configuration= HBaseConfiguration.create
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }
}
