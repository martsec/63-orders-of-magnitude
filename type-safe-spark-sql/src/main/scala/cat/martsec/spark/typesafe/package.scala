package cat.martsec.spark

import java.sql.Timestamp

package object typesafe {

  case class Person(id: BigInt, user_name: String, first_name: String, last_name: String, password: String,
                    email: String, gender: String, city: String, street: String, street_number: Option[Int], first_ip: String)

  case class Login(person_id: BigInt, ip: String, timestamp: Timestamp)


}
