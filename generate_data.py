import pandas as pd
from faker import Faker
import random
import argparse
import os


def generate_departments(num_departments, output_path):
    fake = Faker()
    departments = [
        {"department_id": department_id, "department_name": fake.company()}
        for department_id in range(1, num_departments + 1)
    ]
    departments_df = pd.DataFrame(departments)
    departments_path = os.path.join(output_path, "departments.csv")
    departments_df.to_csv(departments_path, index=False)
    print(f"Departments data saved to {departments_path}")


def generate_employees(num_employees, num_departments, output_path):
    fake = Faker()
    employees = []
    for employee_id in range(num_employees):
        employees.append(
            {
                "employee_id": employee_id + 1,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "department_id": random.randint(1, num_departments),
                "salary": random.randint(50000, 120000),
            }
        )
    employees_df = pd.DataFrame(employees)
    employees_path = os.path.join(output_path, "employees.csv")
    employees_df.to_csv(employees_path, index=False)
    print(f"Employees data saved to {employees_path}")


def main(num_employees, num_departments, output_path):
    os.makedirs(output_path, exist_ok=True)
    generate_departments(num_departments, output_path)
    generate_employees(num_employees, num_departments, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate employee and department data."
    )
    parser.add_argument(
        "--num_employees",
        type=int,
        default=10000,
        help="Number of employee records to generate (default: 10000)",
    )
    parser.add_argument(
        "--num_departments",
        type=int,
        default=30,
        help="Number of department records to generate (default: 30)",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="data",
        help="Directory path where the CSV files will be saved (default: data)",
    )

    args = parser.parse_args()

    main(args.num_employees, args.num_departments, args.output_path)
