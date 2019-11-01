package cat.martsec.spark

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset}

package object io {
  case class ErrorRecord(application_name: String, application_id: String,
                         time: Timestamp, error_message: String, value: String)
  case class ReadResult(base: DataFrame, success: DataFrame, errors: Dataset[ErrorRecord] ) {
    def unpersist() = base.unpersist()
  }

  implicit class DfReaderImprovements(@transient val dataFrameReader: DataFrameReader) extends Serializable {
    def odl: OdlReader = new OdlReader(dataFrameReader)
  }

}
