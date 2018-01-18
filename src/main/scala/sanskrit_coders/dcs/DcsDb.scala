package sanskrit_coders.dcs

import java.io.{File, PrintWriter}

import dbSchema.dcs._
import dbUtils.jsonHelper
import org.slf4j.LoggerFactory
import sanskrit_coders.db.CouchdbDb
import sanskritnlp.transliteration.transliterator

class DcsDb(val serverLocation: String, val userName: String = null, var password: String = null) {
  val booksDb = new CouchdbDb(serverLocation = serverLocation, userName=userName, password=password, dbName = "dcs_books")
  val sentencesDb = new CouchdbDb(serverLocation = serverLocation, userName=userName, password=password, dbName = "dcs_sentences")
  private val log = LoggerFactory.getLogger(getClass.getName)

  def updateBooksDb(dcsBook: DcsObject): Boolean = {
    val jsonMap = jsonHelper.getJsonMap(dcsBook)
    if (dcsBook.dcsId % 50 == 0) {
      log debug (jsonMap.toString())
    }
    //    sys.exit()
    booksDb.updateDoc(id=dcsBook.getKey, doc=dcsBook)
    return true
  }

  def updateSentenceDb(dcsSentence: DcsSentence): Boolean = {
    val jsonMap = jsonHelper.getJsonMap(dcsSentence)
    if (dcsSentence.dcsId % 50 == 0) {
      log debug (jsonMap.toString())
    }
    //    sys.exit()
    sentencesDb.updateDoc(id=dcsSentence.getKey, doc=dcsSentence)
    return true
  }

  def initialize(): Unit = {
    booksDb.initialize
    sentencesDb.initialize
  }

  def getBookByTitle(title: String): Option[DcsBook] = {
    booksDb.queryViewForKey[DcsBook](designDocId = "_design/book_index", viewId = "book_index", key = title)
  }

  def getBook(id: String) = booksDb.getDoc[DcsBook](id = id)

  def getChapter(id: String) = booksDb.getDoc[DcsChapter](id = id)

  def getSentence(id: String) = sentencesDb.getDoc[DcsSentence](id = id)

  def getSentenceLink(id: String) = f"http://vedavaapi.org:5984/dcs_sentences/sentence_$id"
}

object bookConverter {
  private val log = LoggerFactory.getLogger(getClass.getName)
  private val iastDcsCode = "iastDcs"
  private val dcsDb = new DcsDb(serverLocation = "localhost:5984")

  def dump(title: String, outputExtension: String = "tsv", destScheme: String = transliterator.scriptDevanAgarI): Unit = {
    val outfileStr = s"/home/vvasuki/couchdb-client/data/${title}_$destScheme.$outputExtension"
    val outFileObj = new File(outfileStr)
    new File(outFileObj.getParent).mkdirs
    val destination = new PrintWriter(outFileObj)

    val book = dcsDb.getBookByTitle(title = title).get
    val chapters = book.chapterIds.get.map(id => dcsDb.getChapter(id = s"DcsChapter_$id").get)
    val chapterSentences = chapters.zipWithIndex.map({ case (chapter: DcsChapter, chapterId: Int) =>
      val sentences = chapter.sentenceIds.get.map(id => dcsDb.getSentence(id = s"sentence_$id").get).map(_.transliterate(destScheme = destScheme))
      outputExtension match {
        case "tsv" => {
          val tsvLines = sentences.zipWithIndex.map({ case (sentence: DcsSentence, sentenceNumber: Int) =>
            val sentence_id = chapter.sentenceIds.get.apply(sentenceNumber)
            val analysisText = sentence.dcsAnalysisDecomposition.getOrElse(Seq(Seq(DcsWord(root = "NOTHING", dcsId = 0)))).zipWithIndex.map({ case (wordGroup: Seq[DcsWord], wordGroupIndex: Int) =>
              wordGroup.zipWithIndex.map({ case (word: DcsWord, intraGroupIndex: Int) =>
                s"${word.root} {${wordGroupIndex + 1}.${intraGroupIndex + 1}}"
              })
            }).flatten.mkString(";; ")
            val devText = transliterator.transliterate(in_str = sentence.text, destScheme = transliterator.scriptDevanAgarI, sourceScheme = iastDcsCode)
            Seq(s"${chapterId + 1}-${sentenceNumber + 1}", devText, analysisText, dcsDb.getSentenceLink(id = s"sentence_$sentence_id")).mkString("\t")
          })
          tsvLines.foreach(destination.println)
        }
        case _ => {}
      }
      sentences
    })
    if (outputExtension == "json") {
      destination.println(jsonHelper.asString(chapterSentences))
    }

    destination.close()
  }

  def main(args: Array[String]): Unit = {
    dcsDb.initialize
    //    log debug transliterator.transliterate(in_str = "siddhi", destScheme = transliterator.scriptDevanAgarI, sourceScheme = iastDcsCode)
    dump(title = "Bhāvaprakāśa", outputExtension = "json", destScheme = "slp")
  }
}
