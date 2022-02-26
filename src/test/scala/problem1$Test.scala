import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class problem1$Test extends AnyFunSuite  with Logging with BeforeAndAfterAll with Serializable {
  val sc = new SparkContext(new SparkConf().setAppName("Problem 1").setMaster("local"))
  override def afterAll() {
    sc.stop()
  }

  test("query_1") {
    problem1.query_1(sc)
  }

  test("query_2") {
    problem1.query_2(sc)
  }
}
