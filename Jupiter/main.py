from fetch import fetchData
from datetime import date

def text_file_to_list(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = [line.strip() for line in file]
        return lines
    except FileNotFoundError:
        return f"Error: File not found at '{file_path}'"
    except Exception as e:
        return f"An error occurred: {e}"

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

if __name__ == "__main__":
    main()
