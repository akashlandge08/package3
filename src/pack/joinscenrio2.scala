package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object joinscenrio2 {




	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

			val data = Seq(
					("A", "AA"),
					("B", "BB"),
					("C", "CC"),
					("AA", "AAA"),
					("BB", "BBB"),
					("CC", "CCC")
					)

			val df = data.toDF("child", "parent")

			df.show()


			val df1 =  df
			
			
			val df2 =  df.withColumnRenamed("child","child1") 
			            .withColumnRenamed("parent","parent1") 
			
			
			df1.show()
			
			
			df2.show()
			
			
			
			val innerjoin = df1.join(df2, df1("parent")===df2("child1"),"inner")
			
			
			innerjoin.show()



			val finaldf = innerjoin.drop("child1")
			                       .withColumnRenamed("parent1","GrandParent")
			                       
			                       
			                       
			                       
			finaldf.show()
			
			
			
			
			

	}

}