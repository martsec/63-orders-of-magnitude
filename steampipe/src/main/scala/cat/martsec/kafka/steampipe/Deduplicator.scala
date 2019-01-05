package cat.martsec.kafka.steampipe

import java.nio.charset.StandardCharsets
import java.util.{Calendar, Properties}

import com.google.common.hash.{HashFunction, Hashing}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

object Deduplicator {
  val TOPIC_INPUT = "/steampipe:duplicated-records"
  val TOPIC_OUTPUT = "/steampipe:no-duplicates"

  // NOTE: In MapR, tables are just paths
  val HBASE_TABLE = Bytes.toBytes("/user/mapr/deduplication-table")
  val HBASE_COL_FAMILY = Bytes.toBytes("dedup")
  val HBASE_COL_NAME = Bytes.toBytes("processed")
  val HBASE_PROCESSED_VALUE = Bytes.toBytes(1)

  def main(args: Array[String]): Unit = {

    // Set up builder and config
    val builder = new StreamsBuilder()
    val streamingConfig = {
      val settings = new Properties()
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-app")
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    // configure HBase
    val table = getHBaseTable


    // Starts stream logic
    val lines: KStream[Array[Byte],String] = builder.stream(TOPIC_INPUT)

    val nonSeenMessages: KStream[Array[Byte],String] = lines
      // If message is null, we consider it not valid
      .filterNot((_, v) => messageEmpty(v))
      // We let pass only the messages that do not exist in our HBase table
      .filter((_,v) => messageToProcess(v, table))

    // We insert the HASHES as key of HBase table
    // ASSUMPTION: connection does not fail
    nonSeenMessages.foreach((_,v) => insertToHBase(v, table))

    // Finally, we write to the output topic
    nonSeenMessages.to(TOPIC_OUTPUT)


    // Starting stream
    val topology: Topology = builder.build()
    val streams: KafkaStreams = new KafkaStreams(topology, streamingConfig)
    streams.start()

  }

  val hasher: HashFunction = Hashing.sha256()
  def hash(string: String): Array[Byte] = hasher.hashString(string, StandardCharsets.UTF_8).asBytes()

  def messageEmpty[T](msg: T): Boolean = msg == null

  def messageToProcess[T](msg: T, hbaseTable: Table ): Boolean = {
    val msgHash = hash(msg.toString)
    // See if the table contains its hash
    val get = new Get(msgHash)
    // Done this way so in a future we could act depending on the result value
    hbaseTable.get(get).isEmpty
  }

  def insertToHBase[T](msg: T, table: Table): Unit = {
    val msgHash = hash(msg.toString) // duplicated just for show
    val put = new Put(msgHash)
    put.addColumn(HBASE_COL_FAMILY,HBASE_COL_NAME,Calendar.getInstance().getTimeInMillis,HBASE_PROCESSED_VALUE)
    table.put(put)
  }

  def getHBaseTable: Table = {
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    connection.getTable(TableName.valueOf(HBASE_TABLE))
  }
}
