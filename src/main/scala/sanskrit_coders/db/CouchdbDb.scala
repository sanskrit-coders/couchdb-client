package sanskrit_coders.db

import dbSchema.dcs.DcsBook
import dbUtils.jsonHelper
import org.ektorp.ViewQuery
import org.ektorp.http.StdHttpClient
import org.ektorp.impl.{StdCouchDbConnector, StdCouchDbInstance}
import org.slf4j.LoggerFactory

import scala.io.StdIn
import scala.reflect.Manifest

class CouchdbDb(val serverLocation: String, val userName: String = null, var password: String = null, val dbName: String) {
  val log = LoggerFactory.getLogger(getClass.getName)
  var db: StdCouchDbConnector = null

  def initialize = {
    var url = s"http://$serverLocation"
    if (userName != null) {
      if (password == null) {
        log.info("Enter password")
        password = StdIn.readLine()
      }
      url = s"http://$userName:$password@$serverLocation"
    }
    val httpClient = new StdHttpClient.Builder().url(url).build
    val dbInstance = new StdCouchDbInstance(httpClient)
    db = new StdCouchDbConnector(dbName, dbInstance)
  }

  def queryViewForKey[T](designDocId: String, viewId: String, key: String)(implicit mf: Manifest[T]): Option[T] = {
    val query = new ViewQuery()
    query.designDocId(designDocId)
    query.viewName(viewId)
    query.key(key)
    query.includeDocs(true)
    val rows = db.queryView(query).getRows
    if (rows.size()  == 0) {
      return None
    } else {
      val docString = rows.get(0).getDoc
//      log debug docString
      return Some(jsonHelper.fromString[T](jsonStr = docString))
    }
  }

  def getDoc[T](id: String)(implicit mf: Manifest[T]): Option[T] = {
    val stream = db.getAsStream(id)
    if (stream == null) {
      return None
    }
    val doc = scala.io.Source.fromInputStream(stream).mkString("")
    val obj = Some(jsonHelper.fromString[T](doc))
    //    log.debug(obj.toString)
    obj
  }

}

object couchdbDbTest {
}