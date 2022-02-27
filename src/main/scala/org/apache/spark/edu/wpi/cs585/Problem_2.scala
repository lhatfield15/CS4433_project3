package org.apache.spark.edu.wpi.cs585

class Problem_2 {

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder
    .appName("Problem2")
    .getOrCreate()

  // Path to data set
  val csvFile="Dataset_Creation/Purchases.csv"

  // Read and create a temporary view
  // Infer schema (note that for larger files you may want to specify the schema)
  val df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csvFile)
  // Create a temporary view
  df.createOrReplaceTempView("P")

  //T1: Filter out (drop) the Purchases from P with a total purchase amount above $600
  val T1 = spark.sql("""SELECT *
FROM P WHERE TransTotal <= 600
""").show(10)

  //2) T2: Over T1, group the Purchases by the Number of Items purchased,
  // and for each group calculate the median, min and max of total amount spent for purchases in that group.
  val T2 = spark.sql("""SELECT TransNumItems ,percentile_approx(TransTotal, 0.5) ,MIN(TransTotal), MAX(TransTotal)
FROM T1 GROUP BY TransNumItems
""").show(10)
}
