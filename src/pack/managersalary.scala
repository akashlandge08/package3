
package pack
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object managersalary {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			


// Define the schema for the DataFrame
val schema = StructType(List(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("salary", IntegerType, nullable = false),
  StructField("managerid", IntegerType, nullable = true)
))

// Create the data
val data = Seq(
  (1, "joe", 70000, 3),
  (2, "henary", 80000, 4),
  (3, "sam", 60000, null),
  (4, "max", 90000, null)
)

// Convert the data to a DataFrame
val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data.map(Row.fromTuple)),
  schema
)

// Show the DataFrame
df.show()


val nulldf=df.na.fill(0).na.fill("NA")
nulldf.show()

val df1=df
df1.show()  

val df2=df.withColumnRenamed("name","empname")
          .withColumnRenamed("id","empid")

          .withColumnRenamed("salary","man_salary")
          .withColumnRenamed("managerid","man_id")

df2.show()




val joindf=df1.join(df2,df1("managerid")===df2("empid"),"left")

joindf.show()


val selfdf= joindf.select("id","name","salary","man_salary")
selfdf.show()


val casedf=selfdf.withColumn("status",expr("case when salary>man_salary then 'needed' else 'not needed' end"))

val filldf= casedf.filter("status='needed'").drop("man_salary","status")

casedf.show()

 filldf.show()

	}}