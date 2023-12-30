package com.example.singer_rec_system.SingerRecSystemApplication.service

import org.springframework.stereotype.Service
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Get}

import scala.util.Try

@Service
class SingerService {
  def getSinger(singername: String): String = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("singers"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(singername))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        ""
      } else {
        val singerName = Bytes.toString(result.getValue(Bytes.toBytes("singer_info"), Bytes.toBytes("singername")))
        singerName
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }

  def getSingerFollow(username: String, singername: String): Boolean = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("user_singer"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(username + '\t' + singername))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        false
      } else {
        // Extract the recommended singers from the row
        val follow = Bytes.toString(result.getValue(Bytes.toBytes("interact"), Bytes.toBytes("follow")))
        if(follow=="1") true
        else false
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }

  def followSinger(username: String, singername: String, follow: Boolean): Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(hbaseConfig)

    try {
      // 更新 user_singer 表
      val userSingerTable = connection.getTable(TableName.valueOf("user_singer"))
      val userSingerRowKey = username + '\t' + singername
      val getSinger = new Get(Bytes.toBytes(userSingerRowKey))
      val singerResult = userSingerTable.get(getSinger)


      val putSinger = new Put(Bytes.toBytes(userSingerRowKey))
      if (!singerResult.isEmpty) {
        val currentRating = Bytes.toString(singerResult.getValue(Bytes.toBytes("rating"), Bytes.toBytes("rating"))).toInt

        if (follow) {
          putSinger.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("follow"), Bytes.toBytes((1).toString))
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((currentRating + 5000).toString))
        } else {
          putSinger.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("follow"), Bytes.toBytes((0).toString))
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((currentRating - 5000).toString))
        }
      }else {
        if(follow) {
          putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("user"), Bytes.toBytes(username))
          putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("singer"), Bytes.toBytes(singername))
          putSinger.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("follow"), Bytes.toBytes((1).toString))
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((5000).toString))
        }
      }
      userSingerTable.put(putSinger)
    } finally {
      connection.close()
    }
  }
}
