package cat.martsec.kafka.steampipe

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Properties

import cat.martsec.kafka.steampipe.schemas.PassengersDiff
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

object JsonToAvroParser {
  val TOPIC_INPUT = "/steampipe:json-data"
  val TOPIC_OUTPUT = "/steampipe:avro-data"

  implicit val formats = Serialization.formats(NoTypeHints)

  def jsonToAvro(json: String): Array[Byte] = {
    val data = read[PassengersDiff](json)
    caseClassToAvro(data)

  }


  /**
    * Implements the serialization from case class to avro formatted binary data
    * @param data case class with the information to encode
    * @return avro encoded message
    */
  def caseClassToAvro(data: PassengersDiff): Array[Byte] = {
    // Used https://github.com/sksamuel/avro4s
    val format = RecordFormat[PassengersDiff]
    // record is a type that implements both GenericRecord and Specific Record
    val record = format.to(data)

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](PassengersDiff.avroSchema)

    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  /**
    * Deserialize avro byte array to case class
    * @param data
    * @return
    */
  def avroToCaseClass(data: Array[Byte]): PassengersDiff = {
    val byteStream = new ByteArrayInputStream(data)
    byteStream.reset()
    val binDecoder = new DecoderFactory().binaryDecoder(byteStream, null)
    val reader = new SpecificDatumReader[GenericRecord](PassengersDiff.avroSchema)
    val record = reader.read(null, binDecoder)
    val format = RecordFormat[PassengersDiff]
    // record is a type that implements both GenericRecord and Specific Record
    format.from(record)
  }





  def main(args: Array[String]): Unit = {
    val stringSerde = Serdes.String
    val byteArraySerde = Serdes.ByteArray
    // Set up builder and config
    val builder = new StreamsBuilder()
    val streamingConfig = {
      val settings = new Properties()
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "json2Avro")
      // Be careful with the default serdes defined here You might need to add Consumed or Produced
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, byteArraySerde.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass.getName)
      settings
    }

    // Application logic
    val lines: KStream[Array[Byte], String] = builder.stream(TOPIC_INPUT)//, Consumed.`with`(byteArraySerde, stringSerde))

    /**
      * The following does not work.
      *
      * {{ val parsed: KStream[Array[Byte], Array[Byte]] = lines.mapValues(jsonToAvro(_)) }}
      *
      * Documentation says:
      *   mapValues is preferable to map because it will not cause data re-partitioning.
      *   However, it does not allow you to modify the key or key type like map does.
      *     https://docs.confluent.io/current/streams/developer-guide/dsl-api.html
      */

    val parsed: KStream[Array[Byte], Array[Byte]] = lines.map(
      (k, v) => new KeyValue[Array[Byte], Array[Byte]](k, jsonToAvro(v))
    )

    parsed.to(TOPIC_OUTPUT, Produced.`with`(byteArraySerde,byteArraySerde))

    // Check that we can deserialize the data
    //parsed.foreach((k, v) => println(k, v, avroToCaseClass(v)))

    // Build stream and start it
    val topology: Topology = builder.build()
    val streams: KafkaStreams = new KafkaStreams(topology, streamingConfig)

    try{
      streams.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      streams.close()
    }


  }
}
