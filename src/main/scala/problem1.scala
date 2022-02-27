import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object problem1 extends Serializable {
  def query_1(sc: SparkContext): Unit = {

    val InfectedSmall = sc.textFile("Dataset_Creation/INFECTED-SMALL.csv")
    val People = sc.textFile("Dataset_Creation/PEOPLE.csv")
    val InfectedLarge = sc.textFile("Dataset_Creation/INFECTED-LARGE.csv")
    InfectedSmall.collect().foreach(println)
    InfectedLarge.collect().foreach(println)
    People.collect().foreach(println)

//    val SMALL = InfectedSmall.map(f => {
//          f.split(",")
//    })

//    println("Get data Using collect")
//    SMALL.collect().foreach(f => {
//      println("Col1:" + f(0) + ",Col2:" + f(1))
//    })

//    val result = sc.parallelize[String](data)
//      .map(x => (x, 1))
//      .reduceByKey((x, y) => x + y)
//      .collect()

//    result.foreach(x => println("%-20s\t%4d".format(x._1, x._2)))
  }

  def query_2(sc: SparkContext): Unit = {

    val InfectedSmall = sc.textFile("Dataset_Creation/INFECTED-SMALL.csv")
    val People = sc.textFile("Dataset_Creation/PEOPLE.csv")
    val InfectedLarge = sc.textFile("Dataset_Creation/INFECTED-LARGE.csv")
//    InfectedSmall.collect().foreach(println)
//    InfectedLarge.collect().foreach(println)
//    People.collect().foreach(println)
  }

}
