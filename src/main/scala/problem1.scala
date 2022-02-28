import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import math.{ sqrt, pow }

object problem1 extends Serializable {

  def get_distance(x1: Float, y1: Float, x2: Float, y2: Float): Double = {
    sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  def filter_infected(person: String, infectedLookup: Array[String]): Boolean = {
    //get person cords
    val personCords = person.split(",")
    val pX = personCords(1).toFloat
    val pY = personCords(2).toFloat
    //see if our person is within 6 ft of an infected person
    infectedLookup.foreach(infected_person => {
      val infPersonCords = infected_person.split(",")
      val x = infPersonCords(1).toFloat
      val y = infPersonCords(2).toFloat
      //if our person was close to an infected, keep him
      if (get_distance(x, y, pX, pY) < 6) {
        return true
      }
    })
    false
  }

  def query_1(sc: SparkContext): Unit = {
    //read files
    val InfectedSmall = sc.textFile("Dataset_Creation/INFECTED-SMALL.csv")
    val People = sc.textFile("Dataset_Creation/PEOPLE.csv")

    //load the infected file to each worker
    val infectedLookup = sc.broadcast(InfectedSmall.collect)
    // filter out any people who were not within 6ft of an infected person
    val result = sc.parallelize[String](People.collect()).filter( person => { filter_infected(person, infectedLookup.value)}).distinct.collect
    result.foreach(println)
  }

  def query_2(sc: SparkContext): Unit = {
    //read files
    val People = sc.textFile("Dataset_Creation/PEOPLE.csv")
    val InfectedLarge = sc.textFile("Dataset_Creation/INFECTED-LARGE.csv")



//    val result = sc.parallelize[String](People, InfectedLarge)

    //      .map(x => (x, 1))
    //      .reduceByKey((x, y) => x + y))
    //      .collect()
  }

}
