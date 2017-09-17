package sanskrit_coders.dcs

import java.io.{File, PrintWriter}

import dbSchema.dcs.{DcsBook, DcsChapter, DcsSentence, DcsWord}
import org.slf4j.LoggerFactory
import sanskrit_coders.db.CouchdbDb
import sanskritnlp.transliteration.transliterator

class DcsBookWrapper(book: DcsBook) {
}

object dcsDb {
  val booksDb = new CouchdbDb(serverLocation = "localhost:5984", dbName = "dcs_books")
  val sentencesDb = new CouchdbDb(serverLocation = "localhost:5984", dbName = "dcs_sentences")
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
  val log = LoggerFactory.getLogger(getClass.getName)
  val iastDcsCode = "iastDcs"

  def dumpBook(title: String): Unit = {
    val outfileStr = s"/home/vvasuki/couchdb-client/data/$title.tsv"
    val outFileObj = new File(outfileStr)
    new File(outFileObj.getParent).mkdirs
    val destination = new PrintWriter(outFileObj)

    val book = dcsDb.getBookByTitle(title = title).get
    val chapters = book.chapterIds.get.map(id => dcsDb.getChapter(id = s"DcsChapter_$id").get)
    chapters.zipWithIndex.foreach({ case (chapter: DcsChapter, chapterId: Int) =>
      val sentences = chapter.sentenceIds.get.map(id => dcsDb.getSentence(id = s"sentence_$id").get)
      val tsvLines = sentences.zipWithIndex.map({ case (sentence: DcsSentence, sentenceNumber: Int) =>
        val sentence_id = chapter.sentenceIds.get.apply(sentenceNumber)
        val analysisText = sentence.dcsAnalysisDecomposition.getOrElse(Seq(Seq(DcsWord(root = "NOTHING", dcsId = 0)))).zipWithIndex.map({case (wordGroup: Seq[DcsWord], wordGroupIndex: Int) =>
            wordGroup.zipWithIndex.map({case (word: DcsWord, intraGroupIndex: Int) =>
              val devWord = transliterator.transliterate(in_str = word.root, destScheme = transliterator.scriptDevanAgarI, sourceScheme = iastDcsCode)
              s"${devWord} {${wordGroupIndex+1}.${intraGroupIndex+1}}"})
        }).flatten.mkString(";; ")
        val devText = transliterator.transliterate(in_str = sentence.text, destScheme = transliterator.scriptDevanAgarI, sourceScheme = iastDcsCode)
        Seq(s"${chapterId+1}-${sentenceNumber+1}", devText, analysisText, dcsDb.getSentenceLink(id = s"sentence_$sentence_id")).mkString("\t")
      })
      tsvLines.foreach(destination.println)
    })

    destination.close()
  }

  def main(args: Array[String]): Unit = {
    dcsDb.initialize
//    log debug transliterator.transliterate(in_str = "siddhi", destScheme = transliterator.scriptDevanAgarI, sourceScheme = iastDcsCode)
    dumpBook(title = "Bhāvaprakāśa")
  }
}
