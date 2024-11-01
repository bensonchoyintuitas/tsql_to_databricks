import os
import shutil
import sys
from datetime import datetime
from convert_tsql_to_databricks import convert_tsql_to_databricks

def process_sql_file(input_path, output_path):
    """Process a single SQL file with proper error handling"""
    try:
        print(f"Processing: {input_path}")
        convert_tsql_to_databricks(input_path, output_path)
        print(f"Successfully converted {input_path}")
    except Exception as e:
        print(f"Error converting {input_path}")
        print(f"Error details: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        trace = traceback.format_exc()
        formatted_trace = trace.replace('\n', '\n-- ')
        with open(output_path, 'w') as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f'-- Error during conversion on: {now}\n')
            f.write(f'-- Error: {str(e)}\n')
            f.write(f'-- Stack trace:\n-- {formatted_trace}\n')
            f.write(f'-- Original file: {input_path}\n')

def process_directory(input_dir, output_dir):
    """Process all SQL files in directory and subdirectories"""
    for root, dirs, files in os.walk(input_dir):
        relative_path = os.path.relpath(root, input_dir)
        output_subdir = os.path.join(output_dir, relative_path)
        os.makedirs(output_subdir, exist_ok=True)

        for file in files:
            if file.lower().endswith('.sql'):
                input_file_path = os.path.join(root, file)
                output_file_path = os.path.join(output_subdir, file)
                process_sql_file(input_file_path, output_file_path)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 convert_folder_tsql_to_databricks_ansi.py input_directory output_directory")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]

    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)

    process_directory(input_directory, output_directory)
