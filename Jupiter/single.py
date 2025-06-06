from parse import Vehicle
from pub import publish, publisher
import os

def list_files_in_directory(folder_path):
  try:
    files = os.listdir(folder_path)
    return [f for f in files if os.path.isfile(os.path.join(folder_path, f))]

  except Exception as e:
    return f"An error occurred: {e}"
  
def main():
    folder = ""
    files = list_files_in_directory(folder)
    count = 0
    for id in files:
        test = Vehicle.from_json_bulk(folder + "/" + id)
        for msg in test:
            publish(repr(msg))
            count += 1
    print(count)

if __name__ == "__main__":
    main()
    publisher.transport.close()