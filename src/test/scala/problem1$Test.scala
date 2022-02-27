import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class problem1$Test extends AnyFunSuite  with Logging with BeforeAndAfterAll with Serializable {

  test("query_1") {
    val sc = new SparkContext(new SparkConf().setAppName("Problem 1").setMaster("local[1]"))
    problem1.query_1(sc)
    sc.stop()
  }

  test("query_2") {
    val sc = new SparkContext(new SparkConf().setAppName("Problem 1").setMaster("local[2]"))
    problem1.query_2(sc)
    sc.stop()
  }
}
