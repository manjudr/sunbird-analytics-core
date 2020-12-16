package org.ekstep.analytics.framework.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresqlService {

  var pg: EmbeddedPostgres = null;
  var connection: Connection = null;
  var stmt: Statement = null;

  def start() {
    println("******** Establishing The Postgress Connection *********")
    pg = EmbeddedPostgres.builder().setPort(65124).start()
    connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
    println("connection.getClientInfo" + connection.getClientInfo)
  }

  def createNominationTable(): Boolean = {
    val tableName: String = "nomination"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    program_id TEXT PRIMARY KEY,
         |    status TEXT)""".stripMargin

    execute(query)
  }

  def execute(sqlString: String): Boolean = {
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def dropTable(tableName: String): Boolean = {
    stmt.execute(s"DROP TABLE $tableName")
  }

  def close() {
    println("******** Closing The Postgress Connection *********")
    stmt.close()
    connection.close()
    pg.close()
  }
}