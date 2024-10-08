package pack
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object fileread {
  
  
   def main (args:Array[String]):Unit={
    
 val conf= new SparkConf().setAppName("first").setMaster("local[*]")
    
  val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data= sc.textFile("file:///E:/test.txt",1)
    data.foreach(println)
    
    
 val flatten=data.flatMap(x=>x.split(","))
 println
 println("=======flatten========")
 flatten.foreach(println)
    
 
  val Replace =flatten.map(x=>x.replace("MR","Spark"))
 println
 println("=======Replace========")
 Replace.foreach(println)
    
    
    val con=Replace.map(x=> "Tech->"+x +" Trainer->sai")
    println
    println("concated list")
    con.foreach(println)
    
    
    
    
    
    
    
      
    
    
    
    
    
    
   }
}