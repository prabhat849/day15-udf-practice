
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15UDFPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF-Practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("========================================")
    println("DAY 15 - UDF PRACTICE")
    println("========================================")

    // 1. Employee salary data - 12 employees
val employees = Seq(
  (1, "Prabhat", 25000.0),
  (2, "Rahul", 45000.0),
  (3, "Amit", 75000.0),
  (4, "Sneha", 120000.0),
  (5, "Priya", 60000.0),
  (6, "Arjun", 28000.0),
  (7, "Neha", 55000.0),
  (8, "Karan", 95000.0),
  (9, "Anjali", 35000.0),
  (10, "Rohit", 85000.0),
  (11, "Pooja", 150000.0),
  (12, "Vikram", 18000.0)
).toDF("id", "name", "salary")

    println("\n===== ORIGINAL EMPLOYEE DATA =====")
    employees.show(false)

    // 2. Create a Scala UDF for salary bands
    val salaryBandUDF = udf((salary: Double) => {
      if (salary < 30000.0) "Low"
      else if (salary < 60000.0) "Medium"
      else if (salary < 100000.0) "High"
      else "Very High"
    })

    // 3. Add a salary_band column using withColumn
    val classifiedEmployees = employees.withColumn(
      "salary_band",
      salaryBandUDF(col("salary"))
    )

    println("\n===== SALARY CLASSIFICATION USING UDF =====")
    classifiedEmployees.show(false)

    // 4. Built-in Spark alternative to salary UDF
    val builtInClassification = employees.withColumn(
      "salary_band",
      when(col("salary") < 30000.0, "Low")
        .when(col("salary") < 60000.0, "Medium")
        .when(col("salary") < 100000.0, "High")
        .otherwise("Very High")
    )

    println("\n===== SALARY CLASSIFICATION USING BUILT-IN FUNCTIONS =====")
    builtInClassification.show(false)

    // 5. Register the UDF for Spark SQL
    spark.udf.register(
      "salary_band",
      (salary: Double) => {
        if (salary < 30000.0) "Low"
        else if (salary < 60000.0) "Medium"
        else if (salary < 100000.0) "High"
        else "Very High"
      }
    )

    employees.createOrReplaceTempView("employees")

    println("\n===== REGISTERED UDF USING SPARK SQL =====")

    spark.sql("""
      SELECT id, name, salary,
             salary_band(salary) AS salary_band
      FROM employees
      ORDER BY id
    """).show(false)
// 6. Customer transaction data - 12 customers
val customers = Seq(
  ("C101", "Asha", 500.0),
  ("C102", "Vikram", 3500.0),
  ("C103", "Neha", 8000.0),
  ("C104", "Rohan", 15000.0),
  ("C105", "Meera", 2000.0),
  ("C106", "Arjun", 750.0),
  ("C107", "Priya", 4500.0),
  ("C108", "Karan", 12000.0),
  ("C109", "Anjali", 950.0),
  ("C110", "Rahul", 6000.0),
  ("C111", "Pooja", 25000.0),
  ("C112", "Rohit", 4200.0)
).toDF("customer_id", "customer_name", "transaction_value")

    // Illustrative rules only; not a real financial risk assessment.
    val customerRiskUDF = udf((amount: Double) => {
      if (amount < 1000.0) "Low Risk"
      else if (amount < 5000.0) "Medium Risk"
      else "High Risk"
    })

    val customerResults = customers.withColumn(
      "risk_category",
      customerRiskUDF(col("transaction_value"))
    )

    println("\n===== CUSTOMER RISK CATEGORY USING UDF =====")
    customerResults.show(false)

    // 7. Built-in Spark functions for customer risk
    val customerBuiltIn = customers.withColumn(
      "risk_category",
      when(col("transaction_value") < 1000.0, "Low Risk")
        .when(col("transaction_value") < 5000.0, "Medium Risk")
        .otherwise("High Risk")
    )

    println("\n===== CUSTOMER RISK USING BUILT-IN FUNCTIONS =====")
    customerBuiltIn.show(false)

    // 8. Summary
    println("\n===== DAY 15 CONCEPTS =====")
    println("UDF: Executes custom user-defined logic.")
    println("withColumn: Adds or replaces a DataFrame column.")
    println("Built-in functions: Prefer these when they fit the requirement.")
    println("SQL registration: Makes a UDF callable by its registered name.")

    println("\nDAY 15 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
