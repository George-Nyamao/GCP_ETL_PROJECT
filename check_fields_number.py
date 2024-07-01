import pandas as pd

# Function to count fields in each row and add a new column
def add_field_count_column(input_csv, output_csv):
    # Read the CSV file
    df = pd.read_csv(input_csv)
    
    # Count the number of fields in each row
    field_counts = df.apply(lambda row: len(row.dropna()), axis=1)
    
    # Add the field count as a new column
    df['Field_Count'] = field_counts
    
    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_csv, index=False)
    
    print(f"Updated CSV file saved to {output_csv}")

# Input and output file paths
input_csv = 'employee.csv'
output_csv = 'employee_data_with_field_count.csv'

# Add the field count column
add_field_count_column(input_csv, output_csv)
