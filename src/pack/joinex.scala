

package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object joinex {


	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._



			val df = Seq(
					(1),
					(1),
					(1)				
					).toDF("id")

			df.show()
			
			
			val nulldf=df.na.fill(0)
			nulldf.show()



			val df1 = Seq(
					(1),
					(1),
					(2),
					(3)
					).toDF("id")


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


	}

}