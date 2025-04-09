from fetch import fetchData
from json import load
from parse import Vehicle

def main():
    ''''
        For testing purposes
    '''
    fetchData("2907", "test.json")

    vcls = Vehicle.from_json_bulk("test.json")
    for v in vcls:
        print(v)


if __name__ == "__main__":
    main()