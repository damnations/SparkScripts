import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._     // StructType
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark._
import org.apache.spark.streaming._  	// StreamingContext

object SparkAssignment1{
	def main(args: Array[String]) {
	
		val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
		val ssc = new StreamingContext(conf, Seconds(10))
		ssc.sparkContext.setLogLevel("Warn")		
		// Create a DStream that will connect to hostname:port, like localhost:9999
		val lines = ssc.socketTextStream("localhost", 9999)
		val words = lines.flatMap(_.split(" "))
		val output = words.filter{word => word.contains("@")}
		val combos = output.map(output => (output, 1))
	        val wordCounts = combos.reduceByKey(_+_) 
	        
		wordCounts.print()
		// Printthefirsttenelementsof eachRDD generatedin
		// thisDStreamto theconsole wordCounts.print()
		ssc.start()             // Startthecomputation
		ssc.awaitTermination()  // Waitfor thecomputationto terminate
	}
}
