import os
import sys
from collections import defaultdict

def preview_sql_files(directory='output'):
    # Store files by directory for organized display
    files_by_dir = defaultdict(list)
    total_files = 0
    
    # Check if directory exists
    if not os.path.exists(directory):
        print(f"\nError: Directory '{directory}' does not exist.")
        return False
    
    # Walk through directory and all subdirectories
    for root, dirs, files in os.walk(directory):
        # Filter for SQL files
        sql_files = [f for f in files if f.endswith('.sql')]
        if sql_files:
            files_by_dir[root] = sql_files
            total_files += len(sql_files)
    
    if total_files == 0:
        print(f"\nNo SQL files found in '{directory}'")
        return False
        
    # Display preview
    print("\nFiles that will be converted to lowercase:")
    print("=========================================")
    for directory, files in files_by_dir.items():
        print(f"\n📁 {directory}")
        for file in files:
            print(f"  └─ {file}")
    
    print(f"\nTotal SQL files found: {total_files}")
    
    # Ask for confirmation
    response = input("\nProceed with conversion? (y/n): ").lower().strip()
    return response == 'y'

def lowercase_sql_files(directory='output'):
    # First preview and get confirmation
    if not preview_sql_files(directory):
        print("Operation cancelled.")
        return
    
    print("\nProcessing files...")
    print("==================")
    
    # Walk through directory and all subdirectories
    for root, dirs, files in os.walk(directory):
        # Filter for SQL files
        sql_files = [f for f in files if f.endswith('.sql')]
        
        for file in sql_files:
            # Get full file path
            file_path = os.path.join(root, file)
            # Get lowercase version of filename
            lowercase_name = os.path.join(root, file.lower())
            
            try:
                # Read the file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Convert content to lowercase
                lowercase_content = content.lower()
                
                # Write the lowercase content back
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(lowercase_content)
                
                # Rename the file if needed
                if file_path != lowercase_name:
                    os.rename(file_path, lowercase_name)
                    print(f"✓ Renamed and converted: {file_path} → {lowercase_name}")
                else:
                    print(f"✓ Converted content: {file_path}")
                    
            except Exception as e:
                print(f"✗ Error processing {file_path}: {e}")

if __name__ == "__main__":
    # Get directory from command line args or use default
    directory = sys.argv[1] if len(sys.argv) > 1 else 'output'
    lowercase_sql_files(directory)