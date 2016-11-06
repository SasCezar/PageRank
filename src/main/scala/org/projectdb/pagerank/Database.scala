package org.projectdb.pagerank

import org.neo4j.driver.v1._
import org.neo4j.spark.Neo4j


class Database(username: String, password: String, neo: Neo4j) {
  val driver: Driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic(username, password))
  val session: Session = driver.session()


  def loadParentsRelationship(filePath: String): this.type = {
    session.run("CREATE INDEX ON :Domain(name)")
    session.run("CREATE INDEX ON :Site(name)")
    session.run("USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///" + filePath + "'as csvimport FIELDTERMINATOR '\\t' MERGE (d:Domain {name:csvimport.Domain}) MERGE (s:Site {name:csvimport.Page}) MERGE (d)-[:PARENT_OF]->(s)")

    this
  }

  def loadNodesLinks(filePath: String): this.type = {
    session.run("USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///" + filePath + "' as csvimport FIELDTERMINATOR '\\t' MERGE (s:Site {name:csvimport.Source}) MERGE (d:Site {name:csvimport.Destination}) MERGE (s)-[:LINKS_TO]->(d)")
    this
  }

  def clearDB(): this.type = {
    session.run("MATCH (n) DETACH DELETE (n)")
    this
  }

  def loadLinksGraph(): Neo4j = {
    val relationQuery =
      """MATCH (pa:Domain)-[:PARENT_OF]->(a:Site)-[:LINKS_TO]->(b:Site)<-[:PARENT_OF]-(pb:Domain)
        |RETURN id(pa),id(pb)
        | """.stripMargin

    neo.rels(relationQuery)
  }

  def savePageRankValue(nodeID: Long, value: Double): this.type = {
    session.run("MATCH (s) WHERE id(s) = " + nodeID + " SET s.page_rank = toFloat(" + value + ")")
    this
  }

  def getPageRank(domain: String): Double = {
    session.run("MATCH (s:Site) WHERE s.name = " + domain + "RETURN s.page_rank").toString.toDouble
  }

  def getPageRank(nodeID: Long): Double = {
    session.run("MATCH (s:Site) WHERE id(s) = " + nodeID + "RETURN s.page_rank").toString.toDouble
  }

  def createSample(): this.type = {
    session.run("CREATE (a:Domain {name:'Facebook'})")
    session.run("CREATE (a:Site {name:'sub_facebook_1'})")
    session.run("CREATE (a:Site {name:'sub_facebook_2'})")
    session.run("CREATE (a:Site {name:'sub_facebook_3'})")

    session.run("CREATE (a:Domain {name:'Google'})")
    session.run("CREATE (a:Site {name:'sub_google_1'})")
    session.run("CREATE (a:Site {name:'sub_google_2'})")
    session.run("CREATE (a:Site {name:'sub_google_3'})")

    session.run("MATCH (n:Site), (m:Domain {name:'Google'}) WHERE n.name =~ '.*sub_google.*' CREATE (m)-[:PARENT_OF]->(n)")
    session.run("MATCH (n:Site), (m:Domain {name:'Facebook'}) WHERE n.name =~ '.*sub_facebook.*' CREATE (m)-[:PARENT_OF]->(n)")
    session.run("MATCH (m:Site {name:'sub_google_1'}), (n:Site {name:'sub_facebook_1'}) CREATE (m)-[:LINKS_TO]->(n)")
    session.run("MATCH (m:Site {name:'sub_google_2'}), (n:Site {name:'sub_facebook_2'}) CREATE (m)-[:LINKS_TO]->(n)")
    this
  }
}