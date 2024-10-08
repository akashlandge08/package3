

package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object dsldeep {


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

			
			
			println
			println("==========select df==========")
			println
			
			
			val seldf = dsldf.select("txn","category")
			
			seldf.show()
			
			println
			println("==========drop df==========")
			println			
			
			val dropdf = dsldf.drop("product")
			
			dropdf.show()
			
			
			println
			println("==========filter  category Not equal 'Exercise'==========")
			println						
			
			
			
			val sfilter = dsldf.filter(
			                           !  ( col("category")==="Exercise" )
			                            )
			
			sfilter.show()
			
			
			
			println
			println("==========filter category='Exercise' and spendby =='cash'==========")
			println				
			
			val twocolfil = dsldf.filter(
			                          col("category")==="Exercise"
			                          and
			                          col("spendby")==="cash"
			    
			          )
			
			twocolfil.show()
			
			
				println
			println("==========filter category='Exercise' or spendby =='cash'==========")
			println				
			
			
			
			val twocolfilor = dsldf.filter(
			                          col("category")==="Exercise" 
			                          or
			                          col("spendby")==="cash" 
			    
			          )
			
			twocolfilor.show()			
			
			
		  println
			println("==========filter category NOT EQUALS'Exercise' and 'Team Sports'==========")
			println					
			
			
			val infilter = dsldf.filter(
			    
			                        ! ( col("category").isin ("Exercise","Team Sports") )
			
			                        )
			
			
			infilter.show()
			
			
			
		  println
			println("==========filter product  like Gymnastics==========")
			println					
						
			
			
			val likefilter = dsldf.filter(col("product").like ("%Gymnastics%"))
			
			
			likefilter.show()
			
			
		  println
			println("==========filter product  is Null==========")
			println						
			
			
			val nullfilter = dsldf.filter(
			                            col("product") isNull
			                            )
			
			
			nullfilter.show()
			
		  println
			println("==========filter product  is NOT Null==========")
			println						
			
			
			val notnullfilter = dsldf.filter(
			                            col("product") isNotNull
			                            )
			
			
			notnullfilter.show()			
			
			val resultdf= dsldf.selectExpr(
			                              "txn",
			                              "txndate",
			                              "amount",
			                              "upper(category) as category 	  ",
			                       "coalesce(product,'zeyo') as product",

			                       "spendby")
			                       
			                       
			                        resultdf.show()
			
			
	


	}

}