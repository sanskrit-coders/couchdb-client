package sanskrit_coders.dcs

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
}

object bookConverter {
  val log = LoggerFactory.getLogger(getClass.getName)

  def dumpBook(): Unit = {
    val book = dcsDb.getBookByTitle(title = "Aṣṭāṅgahṛdayasaṃhitā").get
    val chapters = book.chapterIds.get.map(id => dcsDb.getChapter(id = s"DcsChapter_$id").get)
    chapters.take(1).foreach(chapter => {
      val sentences = chapter.sentenceIds.get.map(id => dcsDb.getSentence(id = s"sentence_$id").get)
      val tsvLines = sentences.zipWithIndex.map({ case (sentence: DcsSentence, sentence_id: Int) =>
        val analysisText = sentence.dcsAnalysisDecomposition.getOrElse(Seq(Seq(DcsWord(root = "NOTHING", dcsId = 0)))).zipWithIndex.map({case (wordGroup: Seq[DcsWord], wordGroupIndex: Int) =>
            wordGroup.zipWithIndex.map({case (word: DcsWord, intraGroupIndex: Int) =>
              val devWord = transliterator.transliterate(in_str = word.root, destScheme = transliterator.scriptDevanAgarI, sourceScheme = "iast")
              s"${devWord} {${wordGroupIndex+1}.${intraGroupIndex+1}}"})
        }).flatten.mkString(";;")
        val devText = transliterator.transliterate(in_str = sentence.text, destScheme = transliterator.scriptDevanAgarI, sourceScheme = "iast")
        Seq(sentence_id+1, devText, analysisText).mkString("\t")
      })
      log debug tsvLines.mkString("\n")
    })
  }

  def main(args: Array[String]): Unit = {
    dcsDb.initialize
    dumpBook()
  }
}
