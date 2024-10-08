
package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object joinscenrio {




	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._



			val df1 = Seq(
					("a", 100),
					("b", 200),
					("c", 300)
					).toDF("name","id")


			df1.show()


			val df2 = Seq(
					(100, 1000),
					(300, 500)
					).toDF("id", "salary")


			df2.show()


			val df3 = Seq(
					(100, 500),
					(200, 1000)
					).toDF("id", "salary1")

			df3.show()	




			val join1 = df1.join(df2 ,Seq("id") , "left" )
			
			println
			println("======= join 1=======")
			println
			
			join1.show()
			
			
			
			val   join2   =  join1.join(df3 ,Seq("id"), "left" )
			
			println
			println("======= join 2=======")
			println			
			
			join2.show()
			
			
			
			
			val repnull  =  join2.withColumn("salary", expr("coalesce(salary,0)"))
			                     .withColumn("salary1", expr("coalesce(salary1,0)")) 
			
			println
			println("======= repnull=======")
			println				
			
			repnull.show()
			
			
			
			val totsalary =  repnull.withColumn("totsal", expr("salary+salary1"))
			
			println
			println("======= totsalary=======")
			println				
			
			totsalary.show()			
			
			
			
			
			val finaldf = totsalary.select("id","name","totsal")
			                       .withColumnRenamed("totsal","salary")
			
			
			println
			println("======= finaldf=======")
			println				
			
			finaldf.show()					
	}
}