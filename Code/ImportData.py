import csv
import concurrent.futures

def read_data(filename):
    try:
        with open('../Datasets/'+filename, newline='\n') as f:
            reader = csv.reader(f)
            data = list(reader)
            data = [int(i[0]) for i in data]
    except:
        print('Error Reading ',filename,' ; Returning Empty List.')
        data = []
    return data

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        ht = executor.submit(read_data,'HundredThousand.csv')
        om = executor.submit(read_data,'OneMillion.csv')
        tm = executor.submit(read_data,'TenMillion.csv')
        fm = executor.submit(read_data,'FiftyMillion.csv')
        hm = executor.submit(read_data,'HundredMillion.csv')
        HundredThousand = ht.result()
        OneMillion = om.result()
        TenMillion = tm.result()
        FiftyMillion = fm.result()
        HundredMillion = hm.result()