package cat.martsec.kafka.steampipe

import java.nio.charset.StandardCharsets
import java.util.Properties

import com.google.common.hash.Hashing
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.KStream


/**
  * This class reads a message from a stream, hashes it and checks against a MapRDB table (Hbase)
  * if the record has already been seen.
  *
  * Purpose: Hasher caused at ingestion or source
  */
object Hasher {


  def main(args: Array[String]): Unit = {
    val TOPIC_INPUT = "/steampipe:duplicated-records"
    val TOPIC_OUTPUT = "/steampipe:no-duplicates"

    // Set up builder and config
    val builder = new StreamsBuilder()
    val streamingConfig = {
      val settings = new Properties()
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "hasher-app")
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    // Application logic
    val hasher = Hashing.sha256()
    def hash(string: String): String = hasher.hashString(string, StandardCharsets.UTF_8).toString

    val lines: KStream[Array[Byte],String] = builder.stream(TOPIC_INPUT)
    val hashed: KStream[Array[Byte],String] = lines.filterNot((_, v) => v == null).mapValues(hash(_))

    hashed.to(TOPIC_OUTPUT)

    // Build stream and start it
    val topology: Topology = builder.build()
    val streams: KafkaStreams = new KafkaStreams(topology, streamingConfig)

    streams.start()


  }

}
