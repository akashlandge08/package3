

package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object dsljoin {


	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._



			val dsldf = Seq(
					("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
					("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
					("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
					("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
					("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
					("00000005", "02-14-2011", 200, "Gymnastics", null, "cash")
					).toDF("txn","txndate","amount","category","product","spendby")              

			dsldf.show()    
			dsldf.printSchema()

			
			
			println
			println("==========select df==========")
			println
			
			
			val seldf = dsldf.select("txn","category")
			
			seldf.show()
			
			
			val resultdf= dsldf.selectExpr(
			                              "txn",
			                              "txndate",
			                              "amount",
			                              "upper(category) as category 	  ",
			                       "coalesce(product,'zeyo') as product",
                                          "spendby")
			                       
			                       
			                        resultdf.show()
			
			
	val resultdf2= dsldf.selectExpr(
			                              "cast (txn as int) as txn",
			                              "split(txndate,'-')[2] as txndate",
			                              "amount+100 as amount",
			                              "upper(category) as category 	  ",
			                       "coalesce(product,'zeyo') as product",
                                          "spendby",
			                       "case when spendby='cash'then 0 else 1 end as status")
			                       
			                       
			                        resultdf2.show()
			                        
			                        
			val powerfuldf =dsldf  
			
			.withColumn("category",expr("upper(category)"))
			.withColumn("txn",expr("cast(txn as int)"))
			.withColumn("txndate",expr("split(txndate,'-')[2]"))
			.withColumn("amount",expr("amount+100"))
			.withColumn("product",expr("coalesce(product,'NA')"))
				.withColumn("status",expr("case when spendby='cash'then 1 else 0 end"))
			.withColumn("spendby",expr("concat(spendby,'-','zeyo')"))
			.withColumnRenamed("txndate", "year")
	
			
			powerfuldf.show()


	}
}