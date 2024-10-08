package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


object dsl {

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


val selfdf = dsldf.select("txn","category")


selfdf.show()


val dropdf = dsldf.drop("product")

dropdf.show()

val fildf= dsldf.filter(col("category")==="Exercise"
    
and
                        col("spendby")==="cash")

fildf.show()
// Multi value filter (in operator)
val fil1 =dsldf.filter(
    
    col("category") isin ("Exercise","Team Sports")
    )

fil1.show()

val fil2 =dsldf.filter(
    
    col("product") like ("%Gymnastics%")
    )

fil2.show()


val fil3= dsldf.filter(col("category")==="Exercise"
    
or
                        col("spendby")==="cash")

fil3.show()

val fil4 =dsldf.filter(
    
    col("product") like ("%Gymnastics%")
    
    and 
    
    col("amount")>100)
    
   fil4.show()
   
   
   val fil5= dsldf.filter(col("category")==="Exercise"
    
and
                        col("spendby")==="credit")

fil5.show()



}

}