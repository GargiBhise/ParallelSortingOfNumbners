#!/usr/bin/env python3

#Importing Required Libraries
import random
import concurrent.futures
from datetime import datetime

def generate_data(size,filename):
  try:
    # Getting Numbers for Given Range
    data = []
    for i in range(1,size+1):
      data.append(str(i))

    # Randomly Shuffling the Dataset
    random.shuffle(data)

    # Saving the Generated Data in a CSV File
    f = open('../Datasets/'+filename,'w')
    for ele in data:
      f.write(ele)
      f.write('\n')
    print('Successfully Generated '+filename+'\n')
  except:
    print('Error Generating '+filename+'\n')

start = datetime.now()
# Generate Datasets
with concurrent.futures.ProcessPoolExecutor() as executor:
  executor.submit(generate_data,100000,'HundredThousand.csv')
  executor.submit(generate_data,1000000,'OneMillion.csv')
  executor.submit(generate_data,10000000,'TenMillion.csv')
  executor.submit(generate_data,100000000,'HundredMillion.csv')
end = datetime.now()
time = end - start
time = time.total_seconds()
print('Total Time to Generate Datasets :',time)