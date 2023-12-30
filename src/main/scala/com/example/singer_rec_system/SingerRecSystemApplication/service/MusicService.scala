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
class MusicService {
  def getMusic(musicname: String, singername: String): Array[String] = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("musics"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(musicname+'\t'+singername))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        Array.empty[String]
      } else {
        // Extract the recommended singers from the row
        val musicName = Bytes.toString(result.getValue(Bytes.toBytes("music_info"), Bytes.toBytes("musicname")))
        val singerName = Bytes.toString(result.getValue(Bytes.toBytes("music_info"), Bytes.toBytes("singername")))
        Array(musicName, singerName)
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }

  def getMusicLike(username: String, musicname: String, singername: String): Boolean = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("user_music"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(username + '\t' + musicname + '\t' + singername))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        false
      } else {
        // Extract the recommended singers from the row
        val like = Bytes.toString(result.getValue(Bytes.toBytes("interact"), Bytes.toBytes("like")))
        if(like=="1") true
        else false
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }

  def likeMusic(username: String, musicname: String, singername: String, like: Boolean): Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(hbaseConfig)

    try {
      // 更新 user_music 表
      val userMusicTable = connection.getTable(TableName.valueOf("user_music"))
      val userMusicRowKey = username+'\t'+musicname+'\t'+singername
      val putMusic = new Put(Bytes.toBytes(userMusicRowKey))
      if(like){
        putMusic.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("like"), Bytes.toBytes((1).toString))
      }else{
        putMusic.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("like"), Bytes.toBytes((0).toString))
      }
      userMusicTable.put(putMusic)

      // 更新 user_singer 表
      val userSingerTable = connection.getTable(TableName.valueOf("user_singer"))
      val userSingerRowKey = username+'\t'+singername
      val getSinger = new Get(Bytes.toBytes(userSingerRowKey))
      val singerResult = userSingerTable.get(getSinger)

      val putSinger = new Put(Bytes.toBytes(userSingerRowKey))
      if (!singerResult.isEmpty) {
        val currentRating = Bytes.toString(singerResult.getValue(Bytes.toBytes("rating"), Bytes.toBytes("rating"))).toInt

        if(like){
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((currentRating+100).toString))
        }else{
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((currentRating-100).toString))
        }
      }else{
        if (like) {
          putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("user"), Bytes.toBytes(username))
          putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("singer"), Bytes.toBytes(singername))
          putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((100).toString))
        }
      }
      userSingerTable.put(putSinger)
    } finally {
      connection.close()
    }
  }

  def playMusic(username: String, musicname: String, singername: String) :Unit = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(hbaseConfig)

    try {
      // 更新 user_music 表
      val userMusicTable = connection.getTable(TableName.valueOf("user_music"))
      val userMusicRowKey = username + '\t' + musicname + '\t' + singername
      val getMusic = new Get(Bytes.toBytes(userMusicRowKey))
      val musicResult = userMusicTable.get(getMusic)

      val putMusic = new Put(Bytes.toBytes(userMusicRowKey))
      if (!musicResult.isEmpty) {
        val currentPlayCnt = Bytes.toString(musicResult.getValue(Bytes.toBytes("interact"), Bytes.toBytes("play_cnt"))).toInt
        print(currentPlayCnt)
        putMusic.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("play_cnt"), Bytes.toBytes((currentPlayCnt + 1).toString))
      }else {
        putMusic.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("user"), Bytes.toBytes(username))
        putMusic.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("music"), Bytes.toBytes(singername))
        putMusic.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("singer"), Bytes.toBytes(singername))
        putMusic.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("play_cnt"), Bytes.toBytes((1).toString))
      }
      userMusicTable.put(putMusic)
      // 更新 user_singer 表
      val userSingerTable = connection.getTable(TableName.valueOf("user_singer"))
      val userSingerRowKey = username + '\t' + singername
      val getSinger = new Get(Bytes.toBytes(userSingerRowKey))
      val singerResult = userSingerTable.get(getSinger)

      val putSinger = new Put(Bytes.toBytes(userSingerRowKey))
      if (!singerResult.isEmpty) {
        val currentRating = Bytes.toString(singerResult.getValue(Bytes.toBytes("rating"), Bytes.toBytes("rating"))).toInt
        putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((currentRating + 1).toString))
      }else {
        putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("user"), Bytes.toBytes(username))
        putSinger.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("singer"), Bytes.toBytes(singername))
        putSinger.addColumn(Bytes.toBytes("rating"), Bytes.toBytes("rating"), Bytes.toBytes((1).toString))
      }
      userSingerTable.put(putSinger)
    } finally {
      connection.close()
    }
  }
}
