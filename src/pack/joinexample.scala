package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
object JoinExample {
 def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Single Column Seq with Nulls using String")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    // Import implicits to enable toDF method
    import spark.implicits._

    // Define a sequence of data (single column) with null values
    val data: Seq[Option[String]] = Seq(Some("a"), None, Some("c"), Some("d"), None)

    
     val data1: Seq[Option[String]] = Seq(Some("a"),  None,  None, None)

    // Convert the sequence to a DataFrame with a single column
    val df = data.toDF("column1")
    val df1 = data1.toDF("column1")

    // Show the DataFrame
    df.show()

   
			println("left join")
		
			
			
			
			
		val LEFTJOIN = df.join(df1, df("column1") === df1("column1"), "outer")
		LEFTJOIN.show()
		
		
		
		println
			println("right join")
			println
			
			
			
			
			val rightdf = df.join(df1, df("column1") === df1("column1"), "right")
		rightdf.show()
  }
}



    
  
