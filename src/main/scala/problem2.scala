//package org.apache.spark.edu.wpi.cs585

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hive.serde2
object problem2 extends Serializable{

  def task_1(sc: SparkContext): Unit = {
    val spark = SparkSession
      .builder
      .appName("Problem2")
      .getOrCreate()

    // Path to data set
    val csvFileP = "Dataset_Creation/Purchases.csv"
    val schemaP = "TransID INT, CustID INT, TransTotal FLOAT, TransNumItems INT, TransDesc STRING"

    val dfP = spark.read.format("csv")
      .schema(schemaP)
      .load(csvFileP)
    // Create a temporary view
    dfP.createOrReplaceTempView("P")

    // Path to data set
    val csvFileC = "Dataset_Creation/Customers.csv"
    val schemaC = "ID INT, Name STRING, Age INT, CountryCode INT, Salary FLOAT"

    val dfC = spark.read.format("csv")
      .schema(schemaC)
      .load(csvFileC)
    // Create a temporary view
    dfC.createOrReplaceTempView("C")

     //T1: Filter out (drop) the Purchases from P with a total purchase amount above $600
    val T1 =
      spark.sql(
      """SELECT *
      FROM P WHERE TransTotal <= 600
      """).show(10)

    //2) T2: Over T1, group the Purchases by the Number of Items purchased,
    // and for each group calculate the median, min and max of total amount spent for purchases in that group.

    val T2 = spark.sql(
      """SELECT TransNumItems ,percentile_approx(TransTotal, 0.5) ,MIN(TransTotal), MAX(TransTotal)
    FROM P WHERE TransTotal <= 600 GROUP BY TransNumItems
    """).show(10)

    //2) T3  : Over T1, group the Purchases from P by customer ID for young customers between 18 and 25 years of age,
    // and for each group report the customer ID, their age,
    // and total number of items that this person has purchased and total amount spent by the customer.
    val T3 =
      spark.sql(
        """SELECT CustID, MIN(Age), SUM(TransNumItems), SUM(TransTotal)
      FROM P join C on CustID = ID WHERE TransTotal <= 600 and Age BETWEEN 18 and 25 GROUP BY CustID
      """).show(10)


    spark.sql("CREATE TABLE T3 as SELECT CustID, MIN(Age) as Age , SUM(TransNumItems) as TotalItems, SUM(TransTotal) TotalSpend FROM P join C on CustID = ID WHERE TransTotal <= 600 and Age BETWEEN 18 and 25 GROUP BY CustID")
    //5) T4: Select all customer pairs IDs (C1 and C2) from T3
    // where C1 is younger in age than customer C2
    // but C1 spent in total more money than C2 but bought less items.
    val T4 =
      spark.sql(
        """SELECT cust1.CustID as C1ID, cust2.CustID as C2ID,
           cust1.Age as Age1, cust2.Age as Age2,
           cust1.TotalSpend as TotalAmount1, cust2.TotalSpend as TotalAmount2,
           cust1.TotalItems as TotalItems1, cust2.TotalItems as TotalItems2
      FROM T3 cust1, T3 cust2 where cust1.age < cust2.age and cust1.TotalSpend > cust2.TotalSpend
      """).show(10)

  }
}
