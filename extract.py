import os
import random
from faker import Faker
import pandas as pd
from google.cloud import storage

# Set up your Google Cloud project ID
project_id = 'gcap-data-pipeline'

# Set up the path to your service account key JSON file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/gmnya/Documents/2ndStorage/GCP_ETL_Project/gcap-data-pipeline-f6531dcc54ac.json'

# Function to generate a random department
def generate_department():
    departments = ["Engineering", "Sales", "Marketing", "Human Resources", "Finance"]
    department = random.choice(departments)
    return department

# Function to generate a random job title with department consideration
def generate_job_title(department):
    job_titles = {
        "Engineering": ["Software Engineer", "Data Scientist", "DevOps Engineer"],
        "Sales": ["Sales Representative", "Account Manager", "Business Development Manager"],
        "Marketing": ["Marketing Manager", "Content Marketing Specialist", "Social Media Marketing Manager"],
        "Human Resources": ["HR Specialist", "Recruiter", "Talent Acquisition Manager"],
        "Finance": ["Accountant", "Financial Analyst", "Controller"],
    }
    return random.choice(job_titles[department])

# Custom function to generate a 10-digit phone number not starting with zero
def generate_phone_number():
    first_digit = random.randint(1, 9)  # Ensure the first digit is not zero
    remaining_digits = [random.randint(0, 9) for _ in range(9)]
    phone_number = [first_digit] + remaining_digits
    formatted_number = f"({phone_number[0]}{phone_number[1]}{phone_number[2]}){phone_number[3]}{phone_number[4]}{phone_number[5]}-{phone_number[6]}{phone_number[7]}{phone_number[8]}{phone_number[9]}"
    return formatted_number

# Initialize the Faker object
fake = Faker()

# List of employee data
employee_data = []

# Number of employees to generate
num_employees = 100

# Example domains
domains = ['example.com', 'example.net', 'example.org', 'example.edu']

# Generate a list of employees
for _ in range(num_employees):
    department = generate_department()
    first_name = fake.first_name()
    last_name = fake.last_name()
    employee = {
        'First Name': first_name,
        'Last Name': last_name,
        'Job Title': generate_job_title(department),
        'Department': department,
        'Email': first_name[0].lower() + '.' + last_name.lower() + '@' + random.choice(domains),
        'Date of Birth': fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d').replace('\n', '').replace('\r', ''),
        'Phone Number': generate_phone_number(),
        'Address': fake.address().replace('\n', ', ').replace('\r', ', '),
        'Salary': round(fake.random_number(digits=5), 2),
        'Password': fake.password(length=12, special_chars=False, digits=True, upper_case=True, lower_case=True)
    }
    employee_data.append(employee)


# Convert to a pandas DataFrame
df = pd.DataFrame(employee_data)

# Save to a tsv file
tsv_file = 'employee.tsv'
df.to_csv(tsv_file, index=False, sep='\t')

print('Dummy employee data --> employee.tsv')

# Function to upload the tsv file to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Set your GCS bucket name
bucket_name = 'bkt-morara-employee-data'

# Upload the tsv file to GCS
upload_to_gcs(bucket_name, tsv_file, tsv_file)
