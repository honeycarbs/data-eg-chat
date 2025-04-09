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
    for id in list:
        fetchData(id, id + "-" + today + ".json")

if __name__ == "__main__":
    main()
