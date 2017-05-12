package com.datastax.powertools.analytics

/**
  * Created by sebastianestevez on 4/13/17.
  */

import org.apache.spark.ml.clustering.KMeans
import com.datastax.powertools.analytics.ddl.DSECapable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

class SparkMLPredictiveMaintenenceBatchJob {
}

object SparkMLPredictiveMaintenenceBatchJob extends DSECapable{
  def main(args: Array[String]): Unit = {

    case class Observation(user: Int, item: Int, preference: Float)
    def parseObservations(row: Row): Observation = {
      Observation(row.getInt(0), row.getInt(1), row.getDouble(2).toFloat)
    }

    // Create the context
    val sc = connectToDSE("PredictiveMaintenence - Batch")
    //setupSchema("recommendations", "predictions", "(user int, item int, preference float, prediction float, PRIMARY KEY((user), item))")
    val sqlContext = new SQLContext(sc)
    val observations = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load("dsefs:///maintenance_data.csv")

    //get training set
    val Array(training, test) = observations.randomSplit(Array(0.8, 0.2))

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(training)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

  }

  var conf: SparkConf = _
  var sc: SparkContext = _
}