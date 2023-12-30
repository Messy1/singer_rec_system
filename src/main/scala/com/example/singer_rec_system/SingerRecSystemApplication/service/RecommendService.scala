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
class RecommendService {
  def updateRecommend(): Unit = {
    // 创建SparkSession对象
    val spark = SparkSession
      .builder()
      .appName("Read Rating")
      .master("local")
      .getOrCreate()

    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set(TableInputFormat.INPUT_TABLE, "user_singer")

    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //    println(hbaseRDD.count())
    import spark.implicits._

    val rating = hbaseRDD.map({ case (_, result) =>
      val user = Bytes.toString(result.getValue("entity".getBytes, "user".getBytes))
      val singer = Bytes.toString(result.getValue("entity".getBytes, "singer".getBytes))
      val ratingStr = Bytes.toString(result.getValue("rating".getBytes, "rating".getBytes))
      (user, singer, ratingStr)
    }).filter { case (user, singer, ratingStr) =>
      user != null && singer != null && ratingStr != null
    }.toDF("user", "singer", "ratingStr")

    val ratingDF = rating.withColumn("rating", col("ratingStr").cast("integer")).drop("ratingStr")


    val userIndexer = new StringIndexer()
      .setInputCol("user")
      .setOutputCol("userIndex")
      .fit(ratingDF)

    val singerIndexer = new StringIndexer()
      .setInputCol("singer")
      .setOutputCol("singerIndex")
      .fit(ratingDF)

    val indexedRating = singerIndexer.transform(userIndexer.transform(ratingDF))

    //als
    val alsRating = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userIndex")
      .setItemCol("singerIndex")
      .setRatingCol("rating")

    val model = alsRating.fit(indexedRating)
    val singerRecs = model.recommendForAllUsers(5)

    val ratedSingers = indexedRating.select("userIndex", "singerIndex").distinct()
    val recommendations = singerRecs.withColumn("rec", explode(col("recommendations")))
      .select(col("userIndex"), col("rec.singerIndex"), col("rec.rating"))
    val newRecommendations = recommendations.join(ratedSingers,
      recommendations("userIndex") === ratedSingers("userIndex") &&
        recommendations("singerIndex") === ratedSingers("singerIndex"),
      "left_anti")

    import org.apache.spark.sql.expressions.Window

    val windowSpec = Window.partitionBy("userIndex").orderBy(col("rating").desc)
    val top5Recommendations = newRecommendations.withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") <= 5)
      .drop("rank")
    val finalRecommendations = top5Recommendations.groupBy("userIndex")
      .agg(collect_list(struct(col("singerIndex"), col("rating"))) as "recommendations")


    val singerConverter = new IndexToString()
      .setInputCol("rec")
      .setOutputCol("recs")
      .setLabels(singerIndexer.labelsArray(0))

    val expandedRecDF = finalRecommendations
      .withColumn("rec_1", col("recommendations")(0)("singerIndex"))
      .withColumn("rec_2", col("recommendations")(1)("singerIndex"))
      .withColumn("rec_3", col("recommendations")(2)("singerIndex"))
      .withColumn("rec_4", col("recommendations")(3)("singerIndex"))
      .withColumn("rec_5", col("recommendations")(4)("singerIndex"))
      .drop("recommendations")
      .withColumnRenamed("rec_1", "rec")
//    println(expandedRecDF.columns.mkString(", "))

    val expandedRec1DF = singerConverter.transform(expandedRecDF)
      .drop("rec")
      .withColumnRenamed("recs", "recs_1")
      .withColumnRenamed("rec_2", "rec")
    //    println(expandedRec1DF.columns.mkString(", "))
    val expandedRec2DF = singerConverter.transform(expandedRec1DF)
      .drop("rec")
      .withColumnRenamed("recs", "recs_2")
      .withColumnRenamed("rec_3", "rec")
    //    println(expandedRec2DF.columns.mkString(", "))
    val expandedRec3DF = singerConverter.transform(expandedRec2DF)
      .drop("rec")
      .withColumnRenamed("recs", "recs_3")
      .withColumnRenamed("rec_4", "rec")
    //    println(expandedRec3DF.columns.mkString(", "))
    val expandedRec4DF = singerConverter.transform(expandedRec3DF)
      .drop("rec")
      .withColumnRenamed("recs", "recs_4")
      .withColumnRenamed("rec_5", "rec")
    //    println(expandedRec4DF.columns.mkString(", "))
    val expandedRec5DF = singerConverter.transform(expandedRec4DF)
      .drop("rec")
      .withColumnRenamed("recs", "recs_5")
    //    println(expandedRec5DF.columns.mkString(", "))

    val userConverter = new IndexToString()
      .setInputCol("userIndex")
      .setOutputCol("user")
      .setLabels(userIndexer.labelsArray(0))

    val recsWithUserNames = userConverter.transform(expandedRec5DF)

    // 展示结果
    //    recsWithUserNames.select("user", "recs_1", "recs_2", "recs_3", "recs_4", "recs_5").show(249)

    // insert into hbase:recommend
    recsWithUserNames.rdd.foreachPartition(p => {
      // 配置 HBase
      val recommendConf = HBaseConfiguration.create()
      recommendConf.set("hbase.zookeeper.quorum", "Master, Slave1")
      recommendConf.set("hbase.zookeeper.property.clientPort", "2181")
      recommendConf.set("hbase.defaults.for.version.skip", "true")
      val hbaseConn = ConnectionFactory.createConnection(recommendConf)
      val resultTable = TableName.valueOf("recommend")

      //获取表连接
      val table = hbaseConn.getTable(resultTable)
      try {
        p.foreach(r => {
          if (r.getAs[String]("user") != null &&
            r.getAs[String]("recs_1") != null &&
            r.getAs[String]("recs_2") != null &&
            r.getAs[String]("recs_3") != null &&
            r.getAs[String]("recs_4") != null &&
            r.getAs[String]("recs_5") != null
          ) {
            val put = new Put(Bytes.toBytes(r.getAs[String]("user")))
            put.addColumn(Bytes.toBytes("user_info"), Bytes.toBytes("username"), Bytes.toBytes(r.getAs[String]("user")))
            for (i <- 1 to 5) {
              put.addColumn(Bytes.toBytes("singer_info"), Bytes.toBytes(s"singer$i"), Bytes.toBytes(r.getAs[String](s"recs_$i")))
            }
            Try(table.put(put)).recover {
              case e: Exception => e.printStackTrace()
            }
          }
        })
      } finally {
        // 关闭资源
        Try(table.close()).recover { case e: Exception => e.printStackTrace() }
        Try(hbaseConn.close()).recover { case e: Exception => e.printStackTrace() }
      }

      table.close()
      hbaseConn.close()
    })

    spark.close()
  }

  def getRecommend(userid: String): Array[String] = {
    // HBase configuration
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master, Slave1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    // Connect to HBase
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("recommend"))

    try {
      // Fetch the row corresponding to the user ID
      val get = new Get(Bytes.toBytes(userid))
      val result = table.get(get)

      if (result.isEmpty) {
        // Handle the case where the user ID does not exist in the table
        Array.empty[String]
      } else {
        // Extract the recommended singers from the row
        val recommendedSingers = (1 to 5).map { i =>
          val columnName = s"singer$i"
          Option(Bytes.toString(result.getValue(Bytes.toBytes("singer_info"), Bytes.toBytes(columnName)))).getOrElse("")
        }.toArray
        // Return the array of recommended singers
        recommendedSingers
      }
    } finally {
      // Close the HBase table and connection
      table.close()
      connection.close()
    }
  }
}
