import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import math.{pow, sqrt}
import scala.collection.mutable.ArrayBuffer

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

  def cartesian_to_square_grid_coords(x: Float, y: Float): (Float, Float) ={
    return ((x - x % 1000) / 1000, (y - y % 1000) / 1000)
  }

  def map_square_people(coord: String): (String, String) ={
    //get the coords
    val coordArr = coord.split(",")
    val x = coordArr(1).toFloat
    val y = coordArr(2).toFloat

    //covert to square coords
    val (square_x, square_y) = cartesian_to_square_grid_coords(x,y)
    return (square_x.toString + "," + square_y.toString, coord + ",healthy")
  }

  def mapped_square_infected(coord: String): TraversableOnce[(String, String)] = {
    val coordArr = coord.split(",")
    val x = coordArr(1).toFloat
    val y = coordArr(2).toFloat

    //set of grid cells to avoid duplicated as strings "x,y", "x1,y1"...
    var grid_cells = scala.collection.mutable.Set[String]()
    //look in each direction for grid cells
    for (delta_x <- -1 to 1) {
      for (delta_y <- -1 to 1) {
        if (delta_x == 0 && delta_y == 0) {
          //do nothing
        }
        else {
          // find the grid cell in this direction
          val new_x = x + delta_x * 6
          val new_y = y + delta_y * 6
          // add it to the set
          val (square_x, square_y) = cartesian_to_square_grid_coords(new_x, new_y)
          grid_cells += square_x.toString + "," + square_y.toString
        }
      }
    }

    val grid_report = grid_cells.map(square => {
      (square, coord + ",infected")
    })
    grid_report
  }

  def reduce_grid_cells(values: Iterable[String]): scala.collection.mutable.Set[String] = {
    //sort the infected and people
    val infected = ArrayBuffer[String]()
    val people = ArrayBuffer[String]()
    values.foreach(value => {
      val split_values = value.split(",")
      val id = split_values(0)
      val x = split_values(1)
      val y = split_values(2)
      val infection_state = split_values(3)
      if(infection_state == "infected"){
        infected += id + "," + x + "," + y
      }
      else{
        val people_string = id + "," + x + "," + y
        people += people_string
      }
    })

    var exposed_people = scala.collection.mutable.Set[String]()
    people.foreach(person => {
      if(filter_infected(person, infected.toArray)) {
        exposed_people += person
      }
    })
    exposed_people
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
    /*
    squares are 1000x1000units, therefore we have 100 squares in a 10x10 grid
    square names are based on coord of bottom left corner of square divided by 1000
        0,2  |  1,2  | 2,2
      ------------------------
        0,1  |  1,1  | 2,1
      ------------------------
        0,0  |  1,0  | 2,0
     */
    //read files
    val People = sc.textFile("Dataset_Creation/PEOPLE.csv")
    val InfectedLarge = sc.textFile("Dataset_Creation/INFECTED-LARGE.csv")
    val mapped_people = sc.parallelize[String](People.collect())
      .map(x => {map_square_people(x)}).distinct()

    val mapped_infected = sc.parallelize[String](InfectedLarge.collect())
      .flatMap(x => {mapped_square_infected(x)}).distinct()

    val combine_all = mapped_people.union(mapped_infected)
      .groupByKey()
      .flatMap(x => reduce_grid_cells(x._2)).distinct()

    combine_all.collect().foreach(println)
  }


}
