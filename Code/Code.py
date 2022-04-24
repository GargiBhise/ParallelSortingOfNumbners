import sys
sys.setrecursionlimit(1000000000)

import random
import csv
from datetime import datetime
import concurrent.futures
import matplotlib.pyplot as plt

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
    print('Successfully Generated '+filename)
  except:
    print('Error Generating '+filename)
start = datetime.now()
# Generate Datasets
print('Generating Datasets')
with concurrent.futures.ProcessPoolExecutor() as executor:
  executor.submit(generate_data,100000,'HundredThousand.csv')
  executor.submit(generate_data,1000000,'OneMillion.csv')
  executor.submit(generate_data,10000000,'TenMillion.csv')
  executor.submit(generate_data,50000000,'FiftyMillion.csv')
  executor.submit(generate_data,100000000,'HundredMillion.csv')
end = datetime.now()
time = end - start
time = time.total_seconds()
print('Total Time to Generate Datasets :',time,'\n')

print('Importing Datasets')
start = datetime.now()
with open('../Datasets/HundredThousand.csv', newline='\n') as f:
    reader = csv.reader(f)
    HundredThousand = list(reader)
    HundredThousand = [int(i[0]) for i in HundredThousand]

with open('../Datasets/OneMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    OneMillion = list(reader)
    OneMillion = [int(i[0]) for i in OneMillion]

with open('../Datasets/TenMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    TenMillion = list(reader)
    TenMillion = [int(i[0]) for i in TenMillion]

with open('../Datasets/FiftyMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    FiftyMillion = list(reader)
    FiftyMillion = [int(i[0]) for i in FiftyMillion]

with open('../Datasets/HundredMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    HundredMillion = list(reader)
    HundredMillion = [int(i[0]) for i in HundredMillion]

end = datetime.now()
time = end - start
time = time.total_seconds()
print('Total Time to Import Datasets :',time,'\n')

def quickSort(arr,process,thread):
    elements = len(arr)
    #Base case
    if elements < 2:
      return arr
    current_position = 0 #Position of the partitioning element
    for i in range(1, elements): #Partitioning loop
        if arr[i] <= arr[0]:
            current_position += 1
            temp = arr[i]
            arr[i] = arr[current_position]
            arr[current_position] = temp
    temp = arr[0]
    arr[0] = arr[current_position] 
    arr[current_position] = temp #Brings pivot to it's appropriate position
    if process == True and thread == True:
      with concurrent.futures.ProcessPoolExecutor() as executor:
        p1 = executor.submit(quickSort,arr[0:current_position],False,True) #Sorts the elements to the left of pivot
        p2 = executor.submit(quickSort,arr[current_position+1:elements],False,True) #sorts the elements to the right of pivot
        left = p1.result()
        right = p2.result()
    elif process == False and thread == True:
      with concurrent.futures.ThreadPoolExecutor() as executor:
        p1 = executor.submit(quickSort,arr[0:current_position],False,False) #Sorts the elements to the left of pivot
        p2 = executor.submit(quickSort,arr[current_position+1:elements],False,False) #sorts the elements to the right of pivot
        left = p1.result()
        right = p2.result()
    elif process == False and thread == False:
      left = quickSort(arr[0:current_position],False,False) #Sorts the elements to the left of pivot
      right = quickSort(arr[current_position+1:elements],False,False) #sorts the elements to the right of pivot
    arr = left + [arr[current_position]] + right #Merging everything together
    return arr

def mergeSort(myList,process,thread):
    if len(myList) > 1:
        mid = len(myList) // 2
        left = myList[:mid]
        right = myList[mid:]
        if process == True and thread==True:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.submit(mergeSort,left,False,True)
                executor.submit(mergeSort,right,False,True)
        elif process == False and thread==True:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(mergeSort,left,False,False)
                executor.submit(mergeSort,right,False,False)
        elif process == False and thread==False:
            mergeSort(left,False,False)
            mergeSort(right,False,False)
        # Two iterators for traversing the two halves
        i = 0
        j = 0
        # Iterator for the main list
        k = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
              # The value from the left half has been used
              myList[k] = left[i]
              # Move the iterator forward
              i += 1
            else:
                myList[k] = right[j]
                j += 1
            # Move to the next slot
            k += 1
        # For all the remaining values
        while i < len(left):
            myList[k] = left[i]
            i += 1
            k += 1
        while j < len(right):
            myList[k]=right[j]
            j += 1
            k += 1

Merge_SPST = {}
# Single-Process Single-Thread Merge Sort
try:
  print('SPST Merge Sort HundredThousand Begin')
  data = HundredThousand
  start = datetime.now()
  mergeSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPST['HundredThousand'] = time.total_seconds()
  print('SPST Merge Sort HundredThousand Done')
except:
  Merge_SPST['HundredThousand'] = -1
  print('SPST Merge Sort HundredThousand Failed')
try:
  print('SPST Merge Sort OneMillion Begin')
  data = OneMillion
  start = datetime.now()
  mergeSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPST['OneMillion'] = time.total_seconds()
  print('SPST Merge Sort OneMillion Done')
except:
  Merge_SPST['OneMillion'] = -1
  print('SPST Merge Sort OneMillion Failed')
try:
  print('SPST Merge Sort TenMillion Begin')
  data = TenMillion
  start = datetime.now()
  mergeSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPST['TenMillion'] = time.total_seconds()
  print('SPST Merge Sort TenMillion Done')
except:
  Merge_SPST['TenMillion'] = -1
  print('SPST Merge Sort TenMillion Failed')
try:
  print('SPST Merge Sort FiftyMillion Begin')
  data = FiftyMillion
  start = datetime.now()
  mergeSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPST['FiftyMillion'] = time.total_seconds()
  print('SPST Merge Sort FiftyMillion Done')
except:
  Merge_SPST['FiftyMillion'] = -1
  print('SPST Merge Sort FiftyMillion Failed')
try:
  print('SPST Merge Sort HundredMillion Begin')
  data = HundredMillion
  start = datetime.now()
  mergeSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPST['HundredMillion'] = time.total_seconds()
  print('SPST Merge Sort HundredMillion Done')
except:
  Merge_SPST['HundredMillion'] = -1
print('SPST Merge Sort : ',Merge_SPST,'\n')

Merge_SPMT = {}
# Single-Process Multi-Thread Merge Sort
try:
  print('SPMT Merge Sort HundredThousand Begin')
  data = HundredThousand
  start = datetime.now()
  mergeSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPMT['HundredThousand'] = time.total_seconds()
  print('SPMT Merge Sort HundredThousand Done')
except:
  Merge_SPMT['HundredThousand'] = -1
  print('SPMT Sort HundredThousand Failed')
try:
  print('SPMT Merge Sort OneMillion Begin')
  data = OneMillion
  start = datetime.now()
  mergeSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPMT['OneMillion'] = time.total_seconds()
  print('SPMT Merge Sort OneMillion Done')
except:
  Merge_SPMT['OneMillion'] = -1
  print('SPMTg Merge Sort OneMillion Failed')
try:
  print('SPMT Merge Sort TenMillion Begin')
  data = TenMillion
  start = datetime.now()
  mergeSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPMT['TenMillion'] = time.total_seconds()
  print('SPMT Merge Sort TenMillion Done')
except:
  Merge_SPMT['TenMillion'] = -1
  print('SPMT Merge Sort TenMillion Failed')
try:
  print('SPMT Merge Sort FiftyMillion Begin')
  data = FiftyMillion
  start = datetime.now()
  mergeSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPMT['FiftyMillion'] = time.total_seconds()
  print('SPMT Merge Sort FiftyMillion Done')
except:
  Merge_SPMT['FiftyMillion'] = -1
  print('SPMT Merge Sort FiftyMillion Failed')
try:
  print('SPMT Merge Sort HundredMillion Begin')
  data = HundredMillion
  start = datetime.now()
  mergeSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_SPMT['HundredMillion'] = time.total_seconds()
  print('SPMT Merge Sort HundredMillion Done')
except:
  Merge_SPMT['HundredMillion'] = -1
print('SPMT Merge Sort : ',Merge_SPMT,'\n')

Merge_MPMT = {}
# Multi-Process Multi-Thread Merge Sort
try:
  print('MPMT Merge Sort HundredThousand Begin')
  data = HundredThousand
  start = datetime.now()
  mergeSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_MPMT['HundredThousand'] = time.total_seconds()
  print('MPMT Merge Sort HundredThousand Done')
except:
  Merge_MPMT['HundredThousand'] = -1
  print('MPMT Merge Sort HundredThousand Failed')
try:
  print('MPMT Merge Sort OneMillion Begin')
  data = OneMillion
  start = datetime.now()
  mergeSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_MPMT['OneMillion'] = time.total_seconds()
  print('MPMT Merge Sort OneMillion Done')
except:
  Merge_MPMT['OneMillion'] = -1
  print('MPMT Merge Sort OneMillion Failed')
try:
  print('MPMT Merge Sort TenMillion Begin')
  data = TenMillion
  start = datetime.now()
  mergeSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_MPMT['TenMillion'] = time.total_seconds()
  print('MPMT Merge Sort TenMillion Done')
except:
  Merge_MPMT['TenMillion'] = -1
  print('MPMT Merge Sort TenMillion Failed')
try:
  print('MPMT Merge Sort FiftyMillion Begin')
  data = FiftyMillion
  start = datetime.now()
  mergeSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_MPMT['FiftyMillion'] = time.total_seconds()
  print('MPMT Merge Sort FiftyMillion Done')
except:
  Merge_MPMT['FiftyMillion'] = -1
  print('MPMT Merge Sort FiftyMillion Failed')
try:
  print('MPMT Merge Sort HundredMillion Begin')
  data = HundredMillion
  start = datetime.now()
  mergeSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Merge_MPMT['HundredMillion'] = time.total_seconds()
  print('MPMT Merge Sort HundredMillion Done')
except:
  Merge_MPMT['HundredMillion'] = -1
  print('MPMT Merge Sort HundredMillion Failed')
print('MPMT Merge Sort : ',Merge_MPMT,'\n')

Quick_SPMT = {}
# Single-Process Multi-Thread Quick Sort
try:
  print('SPMT Quick Sort HundredThousand Begin')
  data = HundredThousand
  start = datetime.now()
  quickSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_SPMT['HundredThousand'] = time.total_seconds()
  print('SPMT Quick Sort HundredThousand Done')
except:
  Quick_SPMT['HundredThousand'] = -1
  print('SPMT Quick Sort HundredThousand Failed')
try:
  print('SPMT Quick Sort OneMillion Begin')
  data = OneMillion
  start = datetime.now()
  quickSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_SPMT['OneMillion'] = time.total_seconds()
  print('SPMT Quick Sort OneMillion Done')
except:
  Quick_SPMT['OneMillion'] = -1
  print('SPMT Quick Sort OneMillion Failed')
try:
  print('SPMT Quick Sort TenMillion Begin')
  data = TenMillion
  start = datetime.now()
  quickSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_SPMT['TenMillion'] = time.total_seconds()
  print('SPMT Quick Sort TenMillion Done')
except:
  Quick_SPMT['TenMillion'] = -1
  print('SPMT Quick Sort TenMillion Failed')
try:
  print('SPMT Quick Sort FiftyMillion Begin')
  data = FiftyMillion
  start = datetime.now()
  quickSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_SPMT['FiftyMillion'] = time.total_seconds()
  print('SPMT Quick Sort FiftyMillion Done')
except:
  Quick_SPMT['FiftyMillion'] = -1
  print('SPMT Quick Sort FiftyMillion Failed')
try:
  print('SPMT Quick Sort HundredMillion Begin')
  data = HundredMillion
  start = datetime.now()
  quickSort(data,False,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_SPMT['HundredMillion'] = time.total_seconds()
  print('SPMT Quick Sort HundredMillion Done')
except:
  Quick_SPMT['HundredMillion'] = -1
  print('SPMT Quick Sort HundredMillion Failed')
print('SPMT Quick Sort : ',Quick_SPMT,'\n')

Quick_MPMT = {}
# Multi-Process Multi-Thread Quick Sort
try:
  print('MPMT Quick Sort HundredThousand Begin')
  data = HundredThousand
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_MPMT['HundredThousand'] = time.total_seconds()
  print('MPMT Quick Sort HundredThousand Done')
except:
  Quick_MPMT['HundredThousand'] = -1
  print('MPMT Quick Sort HundredThousand Failed')
try:
  print('MPMT Quick Sort OneMillion Begin')
  data = OneMillion
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_MPMT['OneMillion'] = time.total_seconds()
  print('MPMT Quick Sort OneMillion Done')
except:
  Quick_MPMT['OneMillion'] = -1
  print('MPMT Quick Sort OneMillion Failed')
try:
  print('MPMT Quick Sort TenMillion Begin')
  data = TenMillion
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_MPMT['TenMillion'] = time.total_seconds()
  print('MPMT Quick Sort TenMillion Done')
except:
  Quick_MPMT['TenMillion'] = -1
  print('MPMT Quick Sort TenMillion Failed')
try:
  print('MPMT Quick Sort FiftyMillion Begin')
  data = FiftyMillion
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_MPMT['FiftyMillion'] = time.total_seconds()
  print('MPMT Quick Sort FiftyMillion Done')
except:
  Quick_MPMT['FiftyMillion'] = -1
  print('MPMT Quick Sort FiftyMillion Failed')
try:
  print('MPMT Quick Sort HundredMillion Begin')
  data = HundredMillion
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_MPMT['HundredMillion'] = time.total_seconds()
  print('MPMT Quick Sort HundredMillion Done')
except:
  Quick_MPMT['HundredMillion'] = -1
  print('MPMT Quick Sort HundredMillion Failed')
print('MPMT Quick Sort : ',Quick_MPMT,'\n')

print('Plotting and Exporting Begin')
x = ['Hundred Thousand','One Million','Ten Million','Fifty Million','Hundred Million']
y0 = [0 for _ in range(len(x))]

Quick_SPST = [-1,-1,-1,-1]
Quick_SPMT = [Quick_SPMT['HundredThousand'],Quick_SPMT['OneMillion'],Quick_SPMT['TenMillion'],Quick_SPMT['FiftyMillion'],Quick_SPMT['HundredMillion']]
Quick_MPMT = [Quick_MPMT['HundredThousand'],Quick_MPMT['OneMillion'],Quick_MPMT['TenMillion'],Quick_MPMT['FiftyMillion'],Quick_MPMT['HundredMillion']]

Merge_SPST = [Merge_SPST['HundredThousand'],Merge_SPST['OneMillion'],Merge_SPST['TenMillion'],Merge_SPST['FiftyMillion'],Merge_SPST['HundredMillion']]
Merge_SPMT = [Merge_SPMT['HundredThousand'],Merge_SPMT['OneMillion'],Merge_SPMT['TenMillion'],Merge_SPMT['FiftyMillion'],Merge_SPMT['HundredMillion']]
Merge_MPMT = [Merge_MPMT['HundredThousand'],Merge_MPMT['OneMillion'],Merge_MPMT['TenMillion'],Merge_MPMT['FiftyMillion'],Merge_MPMT['HundredMillion']]

plt.plot(x, y0, color='black', linestyle='dotted', label='Zero Line')
plt.plot(x, Quick_SPST, color='red', label='Single-Process Single-Thread')
plt.plot(x, Quick_SPMT, color='blue', label='Single-Process Multi-Thread')
plt.plot(x, Quick_MPMT, color='green', label='Multi-Process Multi-Thread')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Quick Sort')
plt.savefig('../Plots/QuickSort.png', dpi=150)
print('QuickSort.png Exported')

plt.clf()

plt.plot(x, y0, color='black', linestyle='dotted', label='Zero Line')
plt.plot(x, Merge_SPST, color='red', label='Single-Process Single-Thread')
plt.plot(x, Merge_SPMT, color='blue', label='Single-Process Multi-Thread')
plt.plot(x, Merge_MPMT, color='green', label='Multi-Process Multi-Thread')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Merge Sort')
plt.savefig('../Plots/MergeSort.png', dpi=150)
print('MergeSort.png Exported')

plt.clf()

plt.plot(x, y0, color='black', linestyle='dotted', label='Zero Line')
plt.plot(x, Quick_SPST, color='red', label='Quick Sort')
plt.plot(x, Merge_SPST, color='blue', label='Merge Sort')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Single-Process Single-Thread')
plt.savefig('../Plots/SPST.png', dpi=150)
print('SPST.png Exported')

plt.clf()

plt.plot(x, y0, color='black', linestyle='dotted', label='Zero Line')
plt.plot(x, Quick_SPMT, color='red', label='Quick Sort')
plt.plot(x, Merge_SPMT, color='blue', label='Merge Sort')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Single-Process Multi-Thread')
plt.savefig('../Plots/SPMT.png', dpi=150)
print('SPMT.png Exported')

plt.clf()

plt.plot(x, y0, color='black', linestyle='dotted', label='Zero Line')
plt.plot(x, Quick_MPMT, color='red', label='Quick Sort')
plt.plot(x, Merge_MPMT, color='blue', label='Merge Sort')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Multi-Process Multi-Thread')
plt.savefig('../Plots/MPMT.png', dpi=150)
print('MPMT.png Exported')
print('Begin Plotting and Exporting Done','\n')