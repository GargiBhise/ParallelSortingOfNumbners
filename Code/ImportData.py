import csv
def read_data(filename):
    try:
        with open('../Datasets/'+filename, newline='\n') as f:
            reader = csv.reader(f)
            data = list(reader)
            data = [int(i[0]) for i in data]
    except:
        data = []
    return data