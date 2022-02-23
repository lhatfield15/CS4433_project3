package org.apache.spark.edu.wpi.cs585

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunMain extends  Serializable{
  def main(args: Array[String]): Unit = {
    val spark = init_sc()
    val sc = spark.sparkContext
    WordCount.run(sc)
    sc.stop()
  }

  def init_sc(): SparkSession ={
    val conf = new SparkConf().setAppName("cs585")
    conf.set("spark.sql.parquet.compression.codec", "uncompressed")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark
  }
}
