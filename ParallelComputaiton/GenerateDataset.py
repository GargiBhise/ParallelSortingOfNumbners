#Importing Required Libraries
import random

def generate_data(size,filename):
  try:
    # Getting Numbers for Given Range
    data = []
    for i in range(1,size+1):
      data.append(str(i))

    # Randomly Shuffling the Dataset
    random.shuffle(data)

    # Saving the Generated Data in a CSV File
    f = open(filename,'w')
    for ele in data:
      f.write(ele)
      f.write('\n')
    print('Successfully Generated',filename)
  except:
    print('Error Generating',filename)


# Generate Datasets
generate_data(1000,'Thousand.csv')
generate_data(100000,'HundredThousand.csv')
generate_data(1000000,'OneMillion.csv')
generate_data(10000000,'TenMillion.csv')
generate_data(100000000,'HundredMillion.csv')

