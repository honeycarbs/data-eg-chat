from fetch import fetchData
from datetime import date
from parse import Vehicle
from pub import publish, publisher
from datetime import datetime
import os


def text_file_to_list(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = [line.strip() for line in file]
        return lines
    
    except Exception as e:
        return f"An error occurred: {e}"
    
def list_files_in_directory(folder_path):
  try:
    files = os.listdir(folder_path)
    return [f for f in files if os.path.isfile(os.path.join(folder_path, f))]
  
  except Exception as e:
    return f"An error occurred: {e}"

def main():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    list = text_file_to_list('id.txt')
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    new_folder_path = os.path.join(script_dir, today)
    
    try:
        os.mkdir(new_folder_path)
        print(f"Folder '{new_folder_path}' created successfully.")
    
    except Exception as e:
        return f"An error occurred: {e}"
    for id in list:
        fetchData(id, today + "/" + id + ".json")
    
    files = list_files_in_directory(new_folder_path)
    count = 0
    for id in files:
        test = Vehicle.from_json_bulk(today + "/" + id)
        for msg in test:
            publish(repr(msg))
            count += 1
    today_date = datetime.now().strftime('%Y-%m-%d')
    file_path = f"sensor_count-{today_date}.txt"

    file = open(file_path, "w")
    file.write(str(count))
    file.close()

if __name__ == "__main__":
    main()
    publisher.transport.close()
