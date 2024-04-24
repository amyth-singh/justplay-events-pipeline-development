import os
import shutil

def duplicate_file(file_path, num_duplicates, destination_dir):
    try:
        # Read the content of the file
        with open(file_path, 'rb') as file:
            content = file.read()
        
        # Create the destination directory if it doesn't exist
        os.makedirs(destination_dir, exist_ok=True)
        
        # Duplicate the file
        for i in range(1, num_duplicates + 1):
            # Construct the new file name
            new_file_name = os.path.join(destination_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_copy{i}{os.path.splitext(file_path)[1]}")
            
            # Write the content to the new file
            with open(new_file_name, 'wb') as new_file:
                new_file.write(content)
                
            print(f"Duplicate {i} created: {new_file_name}")
            
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
file_path = 'student-mat.csv'  # Replace 'example.txt' with the path to your file
num_duplicates = 999  # Change this to the number of duplicates you want
destination_dir = 'C:\\Users\\amith\\Desktop\\duplicates'  # Change this to the destination directory

duplicate_file(file_path, num_duplicates, destination_dir)