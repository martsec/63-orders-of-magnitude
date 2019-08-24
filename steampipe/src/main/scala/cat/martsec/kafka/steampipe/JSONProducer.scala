package cat.martsec.kafka.steampipe

import java.sql.Timestamp
import java.util.{Properties, UUID}

import cat.martsec.kafka.steampipe.schemas.PassengersDiff
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.util.Random

/**
  * This application will produce and send messages to a kafka topic
  *
  * @author martsec
  */
object JSONProducer {
  val TOPIC_OUTPUT = "/steampipe:json-data"

  // Dict to choose random values
  val trainLines = List("R1", "R2", "R3", "R4", "RT1", "L3", "S2", "L7", "RL2", "L5", "L1", "S1")
  def randomLine: String = trainLines(Random.nextInt(trainLines.size))
  def randomPassengerCount: Int = Random.nextInt(100)
  def randomTrainNumber: Int = Random.nextInt(6)
  def randomDiff: PassengersDiff = PassengersDiff(UUID.randomUUID().toString, randomTrainNumber, randomLine,
    new Timestamp(System.currentTimeMillis()), randomPassengerCount, randomPassengerCount)


  def main(args: Array[String]): Unit = {
    val kafkaCfg = {
      val settings = new Properties()
      settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      settings
    }

    val producer = new KafkaProducer[String, String](kafkaCfg)

    while (true) {

      val passengerDiff = randomDiff

      implicit val formats = Serialization.formats(NoTypeHints)
      val value = write(passengerDiff)

      producer.send(new ProducerRecord(TOPIC_OUTPUT, passengerDiff.line, value))
      Thread.sleep(Random.nextInt(1000))

    }
  }

}
