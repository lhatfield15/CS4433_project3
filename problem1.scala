// Query 1:    Given a huge point file called PEOPLE as well as a small point file called INFECTED-small 
// with  the  later  containing  people  known  to  be  infected  with  covid-19.  Then  for  each  infected  person 
// infect-i  in INFECTED-small file, find all people p-j in the PEOPLE file  that were within 6 feet range 
// of  infect-i  and  that  thus  are  now  at  risk  of  possibly  being  infected  as  well  due  to  having  been  close 
// contacts. Return the list of all people from the PEOPLE files that are close contacts (without duplicates). 
val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)

val PEOPLE = spark.read.textFile("/Dataset_Creation/PEOPLE.csv")
val infPEOPLE = spark.read.textFile("/Dataset_Creation/INFECTED-SMALL.csv")
// All of Spark’s file-based input methods, including textFile, support running on directories, compressed files, and wildcards as well. For example, you can use textFile("/my/directory"), textFile("/my/directory/*.txt"), and textFile("/my/directory/*.gz").
 
// Query 2:    Given two huge files PEOPLE and INFECTED-large.  The file INFECTED-large is large 
// because now many people have become infected due to the latest omicron covid-variant. Then for each 
// infected person infect-i  in the INFECTED-large file, count how many people p-j in the PEOPLE file 
// are within 6 feet range of infect-i, i.e., count how many people infect-i has put at risk.   Return the pairs 
// (infect-i, count-close-contacts-of-infect-i).  
