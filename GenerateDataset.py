#Importing Required Libraries
import random

# Getting 10,000,000 Integer Numbers from 1 to 10,000,000
data = [str(i) for i in range(1,100+1)]

# Randomly Shuffling the Data
random.shuffle(data)

# Saving the Generated Data in a CSV File
data = '\n'.join(data)
f = open('Data.csv','w')
f.write(data)
f.close()

