package org.projectdb.pagerank

import org.neo4j.driver.v1._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._


class App {

  def main(args: Array[String]) {
    /*
    val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "spokebest"))
    val session = driver.session()

    session.run("CREATE (a:Person {name:'Scala_Arthur', title:'King'})")
    session.run("CREATE (a:Person {name:'Scala_Guinevere', title:'Quin'})")

    session.run("MATCH (a:Person {name:'Scala_Arthur'})," + " (b:Person {name:'Scala_Guinevere'})" + "CREATE (a)-[:ENGAGED]->(b)")

    val result: StatementResult = session.run("MATCH (a:Person) WHERE a.name = 'Scala_Arthur' RETURN a.name AS name, a.title AS title")
    while (result.hasNext) {
      val record: Record = result.next
      System.out.println(record.get("title").asString + " " + record.get("name").asString)
    }

    session.close()
    driver.close()
    */

    val conf = new SparkConf().setAppName("SparkDBP").setMaster("local")
    conf.set("spark.neo4j.bolt.user", "neo4j")
    conf.set("spark.neo4j.bolt.password", "spokebest")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    val k = distData.reduce((a,b) => a + b)
    println("distData " + distData)
    println("k " + k)

    val neo = Neo4j(sc)

    val rdd = neo.cypher("MATCH (a:Person) RETURN a.name").loadRowRdd
    println("Query RDD " + rdd)
    println("Query count " + rdd.count)

    sc.stop()

  }
}