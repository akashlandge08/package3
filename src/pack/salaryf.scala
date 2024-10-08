package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object salaryf {




	def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
		
			
			val df = Seq(
			    (10022 ,"Adam" ,1 ,"23/04/2022" ,10020,10000), 
			    (10021 ,"John" ,2 ,"14/04/2022" ,10020 ,13000), 
			    (10020 ,"Marie",1, "10/10/2020" ,10001,20000), 
			    (10004 ,  "Mark",2 ,"19/04/2022",10015,10000)
			    )
         .toDF("EmpId","EMpName","DeptId", "HireDate","MgrId", "salary" )
         
     df.show()    
         
         
     
     val windowdata = Window
                     .partitionBy("DeptId")
                     .orderBy(col("salary") desc)
                     
    

   
     
     val rankdf = df.withColumn("denserank", dense_rank() over windowdata)
     
     
     rankdf.show()
     
     
     val finaldf = rankdf.filter(col("denserank")===2)   
                         .drop("denserank")
     
     
     finaldf.show()
     
     
     // Convert HireDate to DateType
val dfWithDate = df.withColumn("HireDate", to_date(col("HireDate"), "dd/MM/yyyy"))

// Calculate the number of days the employee has been with the company
val dfWithDays = dfWithDate.withColumn("DaysWithCompany", datediff(current_date(), col("HireDate")))


 dfWithDays.show()
// Convert days to years (approximate, considering leap years)
val dfWithYears = dfWithDays.withColumn("YearsWithCompany", (col("DaysWithCompany") / 365.25).cast("double"))

// Select relevant columns and show the result
val resultDf = dfWithYears.select("EmpId", "EMpName", "DeptId", "HireDate", "MgrId", "salary", "DaysWithCompany","YearsWithCompany")
resultDf.show()



// Extract month and year of hire date
val dfWithMonthYear = dfWithDate.withColumn("HireMonthYear", date_format(col("HireDate"), "yyyy-MM"))

dfWithMonthYear.show()

// Get the month and year Adam joined
val adamJoinDate = dfWithMonthYear.filter(col("EMpName") === "Adam").select("HireMonthYear").first().getString(0)



// Filter employees who joined in the same month and year as Adam
val vresultDf = dfWithMonthYear.filter(col("HireMonthYear") === adamJoinDate)

// Show the result
vresultDf.show()
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
         

	}

}