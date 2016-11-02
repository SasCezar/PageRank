package org.projectdb.pagerank

import org.neo4j.driver.v1._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.neo4j.cypher.internal.compiler.v2_3.On.Match

//import org.anormcypher._


object App {

  def main(args: Array[String]) {

    val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "napoleone"))
    val session = driver.session()

    /*
    session.run("CREATE (a:Person {name:'Scala_Arthur', title:'King'})")
    session.run("CREATE (a:Person {name:'Scala_Guinevere', title:'Quin'})")

    session.run("MATCH (a:Person {name:'Scala_Arthur'})," + " (b:Person {name:'Scala_Guinevere'})" + "CREATE (a)-[:ENGAGED]->(b)")

    val result: StatementResult = session.run("MATCH (a:Person) WHERE a.name = 'Scala_Arthur' RETURN a.name AS name, a.title AS title")
    while (result.hasNext) {
      val record: Record = result.next
      System.out.println(record.get("title").asString + " " + record.get("name").asString)
    }
    */

    /*
    session.run("CREATE (a:Site {name:'Facebook'})")
    session.run("CREATE (a:Site {name:'sub_facebook_1'})")
    session.run("CREATE (a:Site {name:'sub_facebook_2'})")
    session.run("CREATE (a:Site {name:'sub_facebook_3'})")

    session.run("CREATE (a:Site {name:'Google'})")
    session.run("CREATE (a:Site {name:'sub_google_1'})")
    session.run("CREATE (a:Site {name:'sub_google_2'})")
    session.run("CREATE (a:Site {name:'sub_google_3'})")

    session.run("MATCH (n:Site), (m:Site {name:'Google'})  " + "WHERE n.name =~ '.*sub_google.*'" + "CREATE (m)-[:PARENT]->(n)")
    session.run("MATCH (n:Site), (m:Site {name:'Facebook'})  " + "WHERE n.name =~ '.*sub_facebook.*'" + "CREATE (m)-[:PARENT]->(n)")
    session.run("MATCH (m:Site {name:'sub_google_1'}), (n:Site {name:'sub_facebook_1'})" + "CREATE (m)-[:LINKS]->(n)")
    session.run("MATCH (m:Site {name:'sub_google_2'}), (n:Site {name:'sub_facebook_2'})" + "CREATE (m)-[:LINKS]->(n)")
    */

    //session.close()
    //driver.close()

    val conf = new SparkConf().setAppName("SparkDBP").setMaster("local")
    conf.set("spark.neo4j.bolt.user", "neo4j")
    conf.set("spark.neo4j.bolt.password", "napoleone")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    val k = distData.reduce((a,b) => a + b)
    println("distData " + distData)
    println("k " + k)

    val neo = Neo4j(sc)
    /*
    val rdd = neo.cypher("MATCH (a:Person) RETURN a.name").loadRowRdd
    println("Query RDD " + rdd)
    println("Query count " + rdd.count)
    */

    val graphQuery = "MATCH (pa:Site)-[:PARENT]->(a:Site)-[:LINKS]->(b:Site)<-[:PARENT]-(pb:Site) RETURN id(pa),id(pb)"
    //Create Graphx Graph
    val graph: Graph[Long, String] = neo.rels(graphQuery).loadGraph

    //graph.vertices.collect.foreach(println)
    //graph.vertices.top(2).foreach(f => println(" " + f._1 + " " + f._2))
    //val q1 = "MATCH (s) WHERE id(s) RETURN s.name"

    println("Graph Nodes: " + graph.edges.count())

    val graphRanked = PageRank.run(graph, 2)

    //val x = List()
    //var list = List[Double]()
    var list:List[Double] = Nil

    graphRanked.vertices.top(2).foreach(t => println("Node id : " + t._1 + " Page_Rank : " + t._2))

    //graphRanked.vertices.top(2).foreach(z => list(z._2))
    //val test = List(1,2,3)
    //list.foreach(println(_))

    //Set new PR Values in Neo4j
    graphRanked.vertices.top(2).foreach(z => session.run("MATCH (s) WHERE id(s) = " + z._1 + " SET s.PageRank = " + z._2))

    //val q2 = "MATCH (s) WHERE id(s)=f._1 SET s.PageRank = f._2 RETURN s"
    //session.run("MATCH (s) WHERE id(s) = 3267 SET s.PageRank = 20")

    val q3 = "MATCH (s) WHERE id(s) = 3267 SET s.PageRank = 15"
    //Graph[Long, String] = neo.rels(q3).loadGraph

    //graphRanked.vertices.top(2).foreach(f => session.run(q2))
    //graphRanked.vertices.top(2).foreach(f => println)

    /*
    val q3 = "MATCH (s) WHERE id(s)= 1114 SET s.pagerank = 15"
    val q4 = "MATCH (s) WHERE id(s) = 1114 RETURN s.name"
    //println(" " + neo.cypher(q4))
    session.run(q3)
    */

    session.close()
    driver.close()
    sc.stop()
  }
}