import random
from datetime import datetime
import concurrent.futures

def generate_data(size,filename):
  try:
    data = []
    for i in range(1,size+1):
      data.append(str(i))
    random.shuffle(data)
    f = open('../Datasets/'+filename,'w')
    for ele in data:
      f.write(ele)
      f.write('\n')
  except:
    print('Error Generating ',filename)

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.submit(generate_data,100000,'HundredThousand.csv')
        executor.submit(generate_data,1000000,'OneMillion.csv')
        executor.submit(generate_data,10000000,'TenMillion.csv')
        executor.submit(generate_data,50000000,'FiftyMillion.csv')
        executor.submit(generate_data,100000000,'HundredMillion.csv')