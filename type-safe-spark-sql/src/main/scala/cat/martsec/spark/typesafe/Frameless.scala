/*
package cat.martsec.spark.typesafe

import frameless.TypedDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import frameless.syntax._

object Frameless {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[2]").appName("frameless Spark test").getOrCreate()
    import spark.implicits._

    val persons = spark.read.option("header", true).option("inferSchema", "true")
      .csv("type-safe-spark-sql/src/main/resources/person.csv").as[Person]

    val logins = spark.read.option("header", true).option("inferSchema", "true")
      .csv("type-safe-spark-sql/src/main/resources/logins.csv").as[Login]


    import frameless.functions._                // For literals
    import frameless.functions.nonAggregate._   // e.g., concat, abs
    import frameless.functions.aggregate._      // e.g., count, sum, avg

    val personsTyped = TypedDataset.create(persons)
    val loginsTyped = TypedDataset.create(logins)

    val street = personsTyped.col('street_number)
    val filtered = personsTyped.filter(street =!= None)

    filtered.show().run()

    // SELECT gender, count(*) FROM filtered GROUP BY gender
    val gender =
    filtered.groupBy(filtered('gender)).count

    // SELECT last_name, first_name, gender FROM filtered ORDER BY last_name, first_nam


    // SELECT last_name, first_name, gender FROM filtered ORDER BY last_name, first_name




  }

}
*/
