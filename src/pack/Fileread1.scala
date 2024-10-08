package pack
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Fileread1 {
  
   def main (args:Array[String]):Unit={
    
 val conf= new SparkConf().setAppName("first").setMaster("local[*]")
    
  val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data= sc.textFile("file:///E:/usdata.csv")
    data.take(5).foreach(println)
    
    
    
    
     val lendata=data.filter(x=>x.length>200)
    println
    println("======length data======")
 lendata.foreach(println)
 
    val flatten=lendata.flatMap(x=>x.split(","))
    println
    println("======flatten data======")
 flatten.foreach(println)
    
     val repdata=flatten.map(x=>x.replace("-",""))
    println
    println("======repdata data======")
 repdata.foreach(println)
    
 
  val condata=repdata.map(x=>x + ",zeyo")
    println
    println("======condata data======")
 condata.foreach(println)
 
 
 condata.coalesce(1).saveAsTextFile("file:///E:/resultdata")
    
    
    
    
    
    
    
}
}