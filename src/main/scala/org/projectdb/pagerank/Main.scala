package org.projectdb.pagerank

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import java.io._

object Main {
  val username:String = "neo4j"
  val password:String = "spokebest1"

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkDBP").setMaster("local")
    conf.set("spark.neo4j.bolt.user", username)
    conf.set("spark.neo4j.bolt.password", password)
    val sc = new SparkContext(conf)

    val database = new Database(username, password, Neo4j(sc))

    //database.clearDB()

    val graph: Graph[Long, String] = database.loadLinksGraph().loadGraph

    print(graph.vertices.count() + "\t" + graph.edges.count())
    val rankedGraph = PageRank.run(graph, 5)

    print(rankedGraph.vertices.count() + "\t" + rankedGraph.edges.count())
    val pw = new PrintWriter(new File("C:\\Users\\Darius\\Documents\\Neo4j\\graph.db\\import\\PageRanks.txt" ))
    pw.write("NodeID" + "\t" + "Score"+ "\r\n")
    rankedGraph.vertices.collect().foreach(node =>
      {
        pw.write(node._1 + "\t" + node._2 + "\r\n")
      }
    )
    pw.close()
    database.savePageRankValue("PageRanks.txt")
  }
}