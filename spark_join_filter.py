from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time


def create_spark_session():
    spark = SparkSession.builder.appName("Join and Filter Operations").getOrCreate()
    return spark


def load_data(spark, employees_path, departments_path):
    employees_df = spark.read.csv(employees_path, header=True, inferSchema=True)
    departments_df = spark.read.csv(departments_path, header=True, inferSchema=True)
    return employees_df, departments_df


def join_then_filter(employees_df, departments_df, salary_threshold):
    start_time = time.time()

    joined_df = employees_df.join(departments_df, on="department_id", how="inner")
    filtered_df = joined_df.filter(col("salary") > salary_threshold)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Join then Filter execution time: {execution_time:.4f} seconds")
    return filtered_df


def filter_then_join(employees_df, departments_df, salary_threshold):
    start_time = time.time()

    filtered_employees_df = employees_df.filter(col("salary") > salary_threshold)
    joined_df = filtered_employees_df.join(
        departments_df, on="department_id", how="inner"
    )

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Filter then Join execution time: {execution_time:.4f} seconds")
    return joined_df


def main():
    EMPLOYEES_DATA_PATH = "data/employees.csv"
    DEPARTMENTS_DATA_PATH = "data/departments.csv"

    spark = create_spark_session()

    employees_df, departments_df = load_data(
        spark, EMPLOYEES_DATA_PATH, DEPARTMENTS_DATA_PATH
    )

    SALARY_THRESHOLD = 80000

    result_join_then_filter = join_then_filter(
        employees_df, departments_df, SALARY_THRESHOLD
    )
    result_join_then_filter.show()

    result_filter_then_join = filter_then_join(
        employees_df, departments_df, SALARY_THRESHOLD
    )
    result_filter_then_join.show()

    spark.stop()


if __name__ == "__main__":
    main()
