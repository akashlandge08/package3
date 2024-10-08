package pack
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj {
  
  def main (args:Array[String]):Unit={
    
 val conf= new SparkConf().setAppName("first").setMaster("local[*]")
    
val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data= sc.textFile("file:///E:/test.txt")
    data.foreach(println)
    
    
    
    val spark= SparkSession.builder().getOrCreate()
    val data1= spark.read.csv("file:///E:/usdata.txt")
    data1.show()
  }
  
  
}