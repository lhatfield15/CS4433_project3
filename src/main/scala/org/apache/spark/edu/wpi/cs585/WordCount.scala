package org.apache.spark.edu.wpi.cs585

import org.apache.spark.SparkContext

object WordCount extends Serializable {
  def run(sc: SparkContext): Unit = {
    val data = this.generate_date()

    val result = sc.parallelize[String](data)
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .collect()

    result.foreach(x => println("%-20s\t%4d".format(x._1, x._2)))
  }

  def generate_date(): Array[String] = {
    val desc = "Founded in 1865 in Worcester, WPI was one of the United States' first engineering and technology universities and now has 14 academic departments with over 50 undergraduate and graduate degree programs in science, engineering, technology, management, the social sciences, and the humanities and arts, leading to bachelor's, master's and PhD degrees. WPI's faculty works with students in a number of research areas, including biotechnology, fuel cells, information security, surface metrology, materials processing, and nanotechnology. It is classified among R2: Doctoral Universities High research activity"
    val words = desc.split(" ")
    words
  }
}
