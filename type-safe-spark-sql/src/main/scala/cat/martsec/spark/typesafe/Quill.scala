package cat.martsec.spark.typesafe

import io.getquill.QuillSparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object Quill {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[2]").appName("Quill Spark test").getOrCreate()
    implicit lazy val sqlContext: SQLContext = spark.sqlContext

    import spark.implicits._
    val persons = spark.read.option("header", true).option("inferSchema", "true")
      .csv("type-safe-spark-sql/src/main/resources/person.csv").as[Person]

    val logins = spark.read.option("header", true).option("inferSchema", "true")
      .csv("type-safe-spark-sql/src/main/resources/logins.csv").as[Login]


    val filtered: Dataset[Person] = run {
      liftQuery(persons).filter(_.street_number.nonEmpty)
    }

    println(filtered.queryExecution.simpleString)
    filtered.show()


    // SELECT gender, count(*) FROM filtered GROUP BY gender
    val simple_group = run {
      liftQuery(filtered).groupBy(p => p.gender)
        .map{
          case (gender, person) => (gender, person.size)
        }
    }
    println(simple_group.queryExecution.simpleString)
    simple_group.show()

    // SELECT last_name, first_name, gender FROM filtered ORDER BY last_name, first_name
    val sorted = run {
      liftQuery(filtered).sortBy{ p => (p.last_name, p.first_name)}(Ord.asc)
        .map(p => (p.last_name, p.first_name, p.gender))
    }
    println(sorted.queryExecution.simpleString)
    sorted.show()

    // SELECT last_name, first_name, gender FROM filtered ORDER BY last_name, first_name
    val sorted2 = run {
      liftQuery(filtered).map(p => (p.last_name, p.first_name, p.gender))
        .sortBy{ case (last_name, first_name, gender) => (last_name, first_name)}(Ord.asc)
    }
    // TODO why false?
    println("Equal logic plans: " + sorted2.queryExecution.simpleString equals sorted.queryExecution.simpleString)




    // SELECT l.IP, p.username FROM Logins l INNER JOIN Persons p ON (l.person_id = p.id) SORT BY l.IP
    val usersLoginIp = run {
      liftQuery(logins).join(liftQuery(persons)).on((l, p) => l.person_id == p.id)
        .map{ case (l, p) => (l.ip, p.user_name) }
    }
    usersLoginIp.show()

    // SELECT ip FROM (SELECT distinct ip, person_id FROM Logins) GROUP BY ip HAVING count(person_id) > 1
    val distinctIpUser =run {
      liftQuery(logins).map(l => (l.ip, l.person_id))
        .distinct
    }
    val ipMultipleUsersLogin = run {
      //liftQuery(logins).map(l => (l.ip, l.person_id))
      //  .distinct // DOES NOT WORK
      liftQuery(distinctIpUser)
        .groupBy{ case (ip, _) => ip }
        .map{ case (ip, persons_id) => (ip, persons_id.size) }
        .filter(_._2 > 1)
        .map(_._1)
    }
    ipMultipleUsersLogin.show()

    val potentialMultiaccounts = run {
      liftQuery(usersLoginIp).join(liftQuery(ipMultipleUsersLogin)).on{ case ((ip, _), ip2) => ip == ip2}
        .map{ case (ipUser, _) => ipUser }
        .sortBy(_._1)
    }

    println(potentialMultiaccounts.queryExecution.simpleString)
    potentialMultiaccounts.show()


    /*It fails at runtime  :(
    // SELECT city, gender, count(*) FROM nonEmptyStreetNum GROUP BY city, gender
    val grouped = run {
      liftQuery(filtered).groupBy(p => (p.city, p.gender))
        .map{
        case ((city, gender), person) => (city, gender, person.size)
      }
    } // Open bug https://github.com/getquill/quill/issues/1023 */


  }
}
