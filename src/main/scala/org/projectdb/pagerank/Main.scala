package org.projectdb.pagerank

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

object Main {
  val username:String = "neo4j"
  val password:String = "spokebest1"

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkDBP").setMaster("local")
    conf.set("spark.neo4j.bolt.user", username)
    conf.set("spark.neo4j.bolt.password", password)
    val sc = new SparkContext(conf)

    val database = new Database(username, password, Neo4j(sc))

    database.clearDB().createSample()

    val graph: Graph[Long, String] = database.loadLinksGraph().loadGraph

    val rankedGraph = PageRank.run(graph, 2)

    rankedGraph.vertices.collect().foreach(node =>
      {
        println(node._2)
        database.savePageRankValue(node._1, node._2)
      })
  }
}