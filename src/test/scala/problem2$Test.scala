import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class problem2$Test extends AnyFunSuite  with Logging with BeforeAndAfterAll with Serializable {

  test("task_1") {
    val sc = new SparkContext(new SparkConf().setAppName("Problem 2").setMaster("local[1]"))
    problem2.task_1(sc)
    sc.stop()
  }


}
