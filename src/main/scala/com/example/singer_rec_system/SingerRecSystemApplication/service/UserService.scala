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
class UserService {
  def signUp(username: String, password: String): Boolean = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(hbaseConfig)

    try {
      // 更新 users 表
      val userTable = connection.getTable(TableName.valueOf("users"))
      val userRowKey = username
      val getUser = new Get(Bytes.toBytes(userRowKey))
      val userResult = userTable.get(getUser)

      if (userResult.isEmpty) {
        val putUser = new Put(Bytes.toBytes(userRowKey))
        if (password.nonEmpty)
          putUser.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("password"), Bytes.toBytes(password))
        userTable.put(putUser)
        true
      }else false
    } finally {
      connection.close()
    }
  }

  def logIn(username: String, password: String): Boolean = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("users"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(username))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        false
      } else {
        // Extract the recommended singers from the row
        val truePassword = Bytes.toString(result.getValue(Bytes.toBytes("user_info"), Bytes.toBytes("password")))
        if (truePassword.isEmpty) true
        else if (truePassword==password) true
        else false
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }
}
