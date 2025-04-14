from fetch import fetchData
from datetime import date
from parse import Vehicle
from pub import publish
import os


def text_file_to_list(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = [line.strip() for line in file]
        return lines
    except FileNotFoundError:
        return f"Error: File not found at '{file_path}'"
    except Exception as e:
        return f"An error occurred: {e}"
    
def list_files_in_directory(folder_path):
  try:
    files = os.listdir(folder_path)
    return [f for f in files if os.path.isfile(os.path.join(folder_path, f))]
  except FileNotFoundError:
    return f"Error: Folder '{folder_path}' not found."
  except NotADirectoryError:
    return f"Error: '{folder_path}' is not a directory."

def main():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    list = text_file_to_list('id.txt')
    import os
    
    new_folder_path = "./" + today
    
    try:
        os.mkdir(new_folder_path)
        print(f"Folder '{new_folder_path}' created successfully.")
    except FileExistsError:
        print(f"Folder '{new_folder_path}' already exists.")
    except FileNotFoundError:
        print(f"Parent directory does not exist.")
    for id in list:
        fetchData(id, today + "/" + id + ".json")
    
    files = list_files_in_directory(new_folder_path)

    for id in files:
        test = Vehicle.from_json_bulk(today + "/" + id)
        count = 0
        for msg in test:
            if(count == 0):
                publish(msg)
                count = 1


if __name__ == "__main__":
    main()
