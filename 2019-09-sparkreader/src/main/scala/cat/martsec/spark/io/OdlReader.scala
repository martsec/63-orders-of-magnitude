package cat.martsec.spark.io

import java.sql.Timestamp

import org.apache.spark.sql.types.{StructField, StringType}
import org.apache.spark.sql.{DataFrame, DataFrameReader}


/**
  *
  * @param dataFrameReader
  */
class OdlReader(@transient val dataFrameReader: DataFrameReader) extends Serializable {

  val CORRUPT_RECORD_COL = "_aux_corrupt_record"

  //def load(): Try[DataFrame] = ???


  def avro() = ???

  def parquet() = ???


  def xml(path: String): ReadResult = {
    import com.databricks.spark.xml._
    val df = dataFrameReader.options(Map(
      "mode" -> "PERMISSIVE",
      "columnNameOfCorruptRecord" -> CORRUPT_RECORD_COL
    )).xml(path)
    //format("com.databricks.spark.xml").load(paths.head)

    processErrorProneSource(df)
  }

  /**
    * Loads JSON files and returns the results as a `DataFrame`.
    * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
    * during parsing.
    *   <ul>
    *     <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
    *     field configured by `columnNameOfCorruptRecord`, and sets other fields to `null`. To
    *     keep corrupt records, an user can set a string type field named
    *     `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have the
    *     field, it drops corrupt records during parsing. When inferring a schema, it implicitly
    *     adds a `columnNameOfCorruptRecord` field in an output schema.</li>
    *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
    *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
    *   </ul>
    * </li>
    * <li>`columnNameOfCorruptRecord` (default is the value specified in
    * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
    * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
    *
    * <li>`multiLine` (default `false`): parse one record, which may span multiple lines,
    * per file</li>
    * </ul>
    *
    * @since 2.0.0
    */
  def json(paths: String*): ReadResult = {
    val df = dataFrameReader.options(Map(
      "mode" -> "PERMISSIVE",
      "columnNameOfCorruptRecord" -> CORRUPT_RECORD_COL
    )).json(paths: _*)
    // Needed caching to avoid
    // org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed
    // when the referenced columns only include the internal corrupt record column
    df.cache()

    processErrorProneSource(df)
  }

  def csv(paths: String*): ReadResult = {
    val schema = dataFrameReader.option("enforceSchema",false)
      .csv(paths: _*).schema add StructField(CORRUPT_RECORD_COL, StringType, true)

    val df = dataFrameReader.options(Map(
      "mode" -> "PERMISSIVE",
      "columnNameOfCorruptRecord" -> CORRUPT_RECORD_COL
    )).schema(schema).csv(paths: _*)
    df.cache()
    processErrorProneSource(df) // WARNING all files must have the same schema
  }


  /**
    * Method that contains the logic to process ds partition by partition
    * and union them.
    * Useful for text, xml and similar sources
    */
  private[this] def processPartitionWise(): DataFrame = ???

  /**
    * From csv, json and xml error sources in permissive mode, it returns the ds splitter into
    * good and error rows.
    */
  private[this] def processErrorProneSource(df: DataFrame): ReadResult = {
    val appName = df.sparkSession.sparkContext.appName
    val appId = df.sparkSession.sparkContext.applicationId
    def time = new Timestamp(System.currentTimeMillis())
    import df.sparkSession.implicits._

    val good = df.where(CORRUPT_RECORD_COL+ " is null").drop(CORRUPT_RECORD_COL)
    val errors = df.select(CORRUPT_RECORD_COL).where(CORRUPT_RECORD_COL+ " is not null")
      .map(r => ErrorRecord(appName, appId,time,"Corrupt record",  r.getString(0)) )

    ReadResult(df, good, errors)
  }

}
