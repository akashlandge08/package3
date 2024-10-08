

package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object alljoin {


	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._



			val df = Seq(
					(1, "raj"),
					(2, "ravi"),
					(3, "sai"),
					(5, "rani")
					).toDF("id","name")

			df.show()



			val df1 = Seq(
					(1, "mouse"),
					(3, "mobile"),
					(7, "laptop")
					).toDF("id","product")    


			df1.show()
			
			
			println
			println("Inner join")
			println
			
			
			
			
		val innerdf=df.join(df1,Seq("id"),"inner")
		innerdf.show()
		
		
		
			
			println
			println("left join")
			println
			
			
			
			
		val leftdf=df.join(df1,Seq("id"),"left")
		leftdf.show()
		
		
		
		  println
			println("right join")
			println
			
			
			
			
		val rightdf=df.join(df1,Seq("id"),"right")
		rightdf.show()
		
		
		
		println
			println("full join")
			println
			
			
			
			
		val Fulldf=df.join(df1,Seq("id"),"full").orderBy("id")
		Fulldf.show()
		
		   println
			println("full join desc by id")
			println
			
			
			
			
		val Fulldf1=df.join(df1,Seq("id"),"full").orderBy(col("id").desc)
		Fulldf1.show()
		
		
		
		  println
			println("Cross join")
			println
		
		
val cross=df.crossJoin(df)
cross.show()


	}

}