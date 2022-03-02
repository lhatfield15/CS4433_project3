import org.apache.spark.SparkContext
import math.{pow, sqrt}
import scala.collection.mutable.ArrayBuffer

object problem1 extends Serializable {

  def get_distance(x1: Float, y1: Float, x2: Float, y2: Float): Double = {
    sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  def count_people_within_6ft(person: String, infectedLookup: Array[String]): Int = {
    //get person cords
    val personCords = person.split(",")
    val pX = personCords(1).toFloat
    val pY = personCords(2).toFloat
    var count = 0
    //see if our person is within 6 ft of another person
    infectedLookup.foreach(infected_person => {
      //get infected coords
      val infPersonCords = infected_person.split(",")
      val x = infPersonCords(1).toFloat
      val y = infPersonCords(2).toFloat
      //if our person was close to an infected, keep him
      if (get_distance(x, y, pX, pY) < 6) {
        count += 1
      }
    })
    count
  }

  def cartesian_to_square_grid_coords(x: Float, y: Float): (Float, Float) ={
    return ((x - x % 1000) / 1000, (y - y % 1000) / 1000)
  }

  def mapped_square_infected(coord: String): (String, String) ={
    //get the coords
    val coordArr = coord.split(",")
    val x = coordArr(1).toFloat
    val y = coordArr(2).toFloat

    //covert to square coords
    val (square_x, square_y) = cartesian_to_square_grid_coords(x,y)
    return (square_x.toString + "," + square_y.toString, coord + ",infected")
  }

  def mapped_square_people(coord: String): TraversableOnce[(String, String)] = {
    //get coord of person
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

    //map the square coord to the info of the infected person with an infected tag
    val grid_report = grid_cells.map(square => {
      (square, coord + ",healthy")
    })
    grid_report
  }

  def reduce_grid_cells(values: Iterable[String]): ArrayBuffer[(String, Int)] = {
    //sort the infected and people
    val infected = ArrayBuffer[String]()
    val people = ArrayBuffer[String]()
    values.foreach(value => {
      //get the info of the individual
      val split_values = value.split(",")
      val id = split_values(0)
      val x = split_values(1)
      val y = split_values(2)
      val infection_state = split_values(3)
      //sort individual into infect or person
      if(infection_state == "infected"){
        infected += id + "," + x + "," + y
      }
      else{
        people += id + "," + x + "," + y
      }
    })

    //get the count of the near people for each infected person
    var exposed_people = ArrayBuffer[(String, Int)]()
    infected.foreach(infected => {
      val num_contacts = count_people_within_6ft(infected, people.toArray)
      val value = (infected, num_contacts) // to make scala happy
      exposed_people += value
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
    val result = sc.parallelize[String](People.collect()).filter( person => {
      //only care if theres at least one infected person near (x>0)
      count_people_within_6ft(person, infectedLookup.value) > 0
    }).distinct.collect
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

    //Take InfectedLarge and return (key , value) (coordinate , id,x,y,healthy)
    val mapped_infected = sc.parallelize[String](InfectedLarge.collect())
      .map(x => {mapped_square_infected(x)}).distinct()

    //Take PEOPLE and return multiple (key , value)'s (if in 6ft) (coordinate , id,x,y,infected)
    val mapped_people = sc.parallelize[String](People.collect())
      .flatMap(x => {mapped_square_people(x)}).distinct()

    //Combine all values by key (Key, Arr[Values])
    //Map -> take value array and return number of infected near if any (id,x,y , number)
    val combine_all = mapped_people.union(mapped_infected)
      .groupByKey()
      .flatMap(x => reduce_grid_cells(x._2)).distinct()

    //print
    combine_all.collect().foreach(println)
  }


}
