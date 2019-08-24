package cat.martsec.kafka.steampipe.schemas

import java.sql.Timestamp

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema.Parser

case class PassengersDiff(event: String,
                          trainId: Int,
                          line: String,
                          stopTime: Timestamp,
                          passengerOn: Int,
                          passengersOff: Int)
object PassengersDiff {
  val schema = """
                     |{ "type": "record",
                     |"name":"passengersDiff",
                     |"fields": [
                     |  {"name":"event", "type":"string"},
                     |  {"name":"trainId", "type":"int"},
                     |  {"name":"line", "type":"string"},
                     |  {"name":"stopTime", "type":"long", "logicalType":"timestamp-millis"},
                     |  {"name":"passengerOn", "type":"int"},
                     |  {"name":"passengersOff", "type":"int"}
                     |]
                     |}
                   """.stripMargin
  val avroSchema = new Parser().parse(schema)
  val newSchema = AvroSchema[PassengersDiff]
}


object Main extends App {
  println(PassengersDiff.schema)
  println(PassengersDiff.avroSchema)
  println(PassengersDiff.newSchema)
}
