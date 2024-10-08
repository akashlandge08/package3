package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object antijoin {




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



			val antijoin = df.join(df1,Seq("id"),"left_anti")


			antijoin.show()




			val ids = df1.select("id").rdd.map( x => x.mkString("")).collect.toList


			println(ids)
			println

			val resuldf = df.filter(!(col("id").isin(ids: _*)))


			resuldf.show()



	}

}