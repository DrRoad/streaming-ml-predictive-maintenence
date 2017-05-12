package com.datastax.powertools.analytics

import java.util.Calendar

import org.apache.spark.mllib.clustering.StreamingKMeans
import com.datastax.powertools.analytics.SparkMLPredictiveMaintenenceBatchJob.setupSchema
import com.datastax.powertools.analytics.ddl.DSECapable
import com.datastax.spark.connector._
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


// For DSE it is not necessary to set connection parameters for spark.master (since it will be done
// automatically)

/**
 * https://github.com/brkyvz/streaming-matrix-factorization
 * https://issues.apache.org/jira/browse/SPARK-6407
 */
object SparkMLProductRecommendationServeJDBC extends DSECapable {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SimpleSparkStreaming <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sc = connectToDSE("SparkMLServeClusteringJDBC")

    // Set up schema
    //setupSchema("recommendations", "predictions", "(user int, item int, preference float, prediction float, PRIMARY KEY((user), item))")

    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(5))

    //start the JDBC server to host predictions
    val hiveContext = new HiveContext(sc)
    HiveThriftServer2.startWithContext(hiveContext)


    val model = new StreamingKMeans()
      .setK(2)
      //.setHalfLife(halfLife, timeUnit)
      //.setRandomCenters(numDimensions, 0.0)


    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val observation = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    observation.print

    //perhaps I should sample between train and predict
    val trainStream= observation.map(Vectors.parse)


    model.trainOn(trainStream)

    //now predict against the live stream
    //this gives us predicted ratings for the item user combination fed from the stream
    val predictions = model.predictOn(trainStream)

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length-1)).mkString("\n")
      val predictString = rdd.map(p => p.toString).collect().mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      print(dateString + "-model", modelString)
      print(dateString + "-predictions", predictString)
    }

    //CACHE TABLE table recommendations.predictions once per window (which is once per RDD)
    /*
    observation.foreachRDD(row => {
      //hiveContext.sql("select * from recommendations.predictions").persist()
      //hiveContext.cacheTable("recommendations.predictions")
      hiveContext.sql("use recommendations").collect()
      hiveContext.sql("cache table predictions").collect()
      print(s"Cached the predictions table")
      true
    })
    */

    ssc.start()
    ssc.awaitTermination()
  }

  var conf: SparkConf = _
  var sc: SparkContext = _
}

case class Predictions(user: Int, item: Int, prediction: Float)

// scalastyle:on println