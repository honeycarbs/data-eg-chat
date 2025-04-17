from parse import Vehicle
from pub import publish, publisher
import os

def list_files_in_directory(folder_path):
  try:
    files = os.listdir(folder_path)
    return [f for f in files if os.path.isfile(os.path.join(folder_path, f))]
  except FileNotFoundError:
    return f"Error: Folder '{folder_path}' not found."
  except NotADirectoryError:
    return f"Error: '{folder_path}' is not a directory."
  
def main():
    folder = ""
    files = list_files_in_directory(folder)
    count = 0
    for id in files:
        test = Vehicle.from_json_bulk(folder + "/" + id)
        for msg in test:
            publish(repr(msg))
            count += 1
    publish(str(count))

if __name__ == "__main__":
    main()
    publisher.transport.close()