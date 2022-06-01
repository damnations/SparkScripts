import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._     // StructType
import org.apache.spark._

object SparkAssignment2{
	def main(args: Array[String]) {
	
		// create Spark session
		val spark = SparkSession.builder().appName("SparkAssignment2")
					.master("local[2]").getOrCreate();
		// create Spark context
		val sc=spark.sparkContext
		// set level of terminal logging information
		spark.sparkContext.setLogLevel("Warn")	
		
		// create SQL context
		val sqlContext = new SQLContext(sc) 
		
		// define schema for CSV reading
		
		// read CSV file into a DataFrame
		val csvDF = sqlContext
		 .read
		 .format("csv")   
		 .option("sep", ";")
		 .option("header","true")
		 .option("inferSchema","true")   // automatically defined schema
		 .load("hdfs://localhost:54310/spark/input/NHL_stats_2021-2022.csv") 
		 
		// show schema
	    	csvDF.printSchema() 
		 
		// a)	select players who have done more than 50 goals during season and output the following columns: Player, Season, Team, G
		val result1 = csvDF.select("Player","Season","Team","G").where("G > 50")
		
		// b)	select players who have done more than 30 assists and less than 20 goals during season and output the following columns: Player, Season, Team, G, A
		val result2 = csvDF.select("Player","Season","Team","G","A").where("A > 30 AND G < 20")
		
		// c)	select players whose +/- value is at least 18 and who are left-handed (S/C = “L”) centers (Pos = “C”) and output the following columns: Player, S/C, Pos, +/-
		val result3 = csvDF.select("Player","S/C","Pos","+/-").where("`+/-` >= 18 AND `S/C` = 'L' AND Pos = 'C'")
		 
		 // show result on terminal
		 result1.show()
 		 result2.show()
 		 result3.show()
		 
		 // write result to file
		 result1.write
    	 	.option("header", "true")
    	 	.option("sep", ";")
    	 	.csv("hdfs://localhost:54310/spark/output1/")
		 
		 result2.write
    	 	.option("header", "true")
    	 	.option("sep", ";")
    	 	.csv("hdfs://localhost:54310/spark/output2/")
    	 	
    	 	result3.write
    	 	.option("header", "true")
    	 	.option("sep", ";")
    	 	.csv("hdfs://localhost:54310/spark/output3/")
    	   		 
		 spark.stop()
	}
}
