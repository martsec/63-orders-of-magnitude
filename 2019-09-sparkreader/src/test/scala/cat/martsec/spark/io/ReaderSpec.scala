package cat.martsec.spark.io

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{FunSuite, Matchers}

class ReaderSpec extends FunSuite with DatasetSuiteBase with Matchers {
  override def reuseContextIfPossible: Boolean = true
  private val resName = "2019-09-sparkreader/src/test/resources/"
  private val jsonDir = resName + "json/"
  private val goodJson = jsonDir + "good.json"
  private val badJson = jsonDir + "malformed.json"
  private val xmlDir = resName + "xml/"
  private val csvDir = resName + "csv/"
  private val booksCsv = csvDir + "books.csv"
  private val booksShuffle = csvDir + "bookShufleCols.csv"
  private val booksExtraCsv = csvDir + "booksExtra.csv"

  test("Is an implicit class of DataFrameReader") {
    spark.read.odl shouldBe a [OdlReader]
  }
  /**
    * JSON
    */
  test("Parses json without exceptions") {
    spark.read.odl.json(jsonDir)
  }
  test("Puts good json records to successful df") {
    val json = spark.read.odl.json(jsonDir)
    json.success.count shouldBe 1
  }
  test("Puts corrupt json records to error ds") {
    val json = spark.read.odl.json(jsonDir)
    json.errors.count shouldBe 2
  }
  test("Accepts multiple json paths") {
    val json = spark.read.odl.json(goodJson, badJson)
    json.success.count shouldBe 1
    json.errors.count shouldBe 2
  }
  /**
    * XML
    */
  test("Parses xml without exception") {
    spark.read.option("rowTag","book").odl.xml(xmlDir)
  }
  test("Puts good xml records to successful df") {
    val r = spark.read.option("rowTag","book").odl.xml(xmlDir)
    r.success.count shouldBe 14
  }
  test("Puts corrupt xml records to error ds") {
    val r = spark.read.option("rowTag","book").odl.xml(xmlDir)
    r.errors.count shouldBe 2
  }

  /**
    * CSV
    */
  test("Parses csv without exception") {
    val r = spark.read.odl.csv(booksCsv)
    r.success.show(false)
    r.errors.show(false)
  }
  test("Parses csv files with shuffled columns") {
    val r = spark.read.option("header",true).option("enforceSchema",false)
      .option("nullValue",true).odl.csv(booksShuffle,booksCsv)
    r.success.show(false)
    r.errors.show(false)

    r.success.where("description like 'BookDescription'").count shouldBe 1
  }
  test("Parses csv with extra options") {
    val r = spark.read.option("header",true).option("enforceSchema",false)
      .option("nullValue",true).odl.csv(booksExtraCsv,booksCsv)
    r.success.show(false)
    r.errors.show(false)
  }

}
