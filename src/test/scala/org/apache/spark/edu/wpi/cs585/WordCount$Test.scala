package org.apache.spark.edu.wpi.cs585

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

class WordCount$Test extends AnyFunSuite  with Logging with BeforeAndAfterAll with Serializable {
  val sc = new SparkContext(new SparkConf().setAppName("TEST").setMaster("local"))
  override def afterAll() {
    sc.stop()
  }

  test("run") {
    WordCount.run(sc)
  }
}
