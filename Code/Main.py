import sys
from datetime import datetime
import matplotlib.pyplot as plt
import concurrent.futures

from ImportData import read_data
from QuickSort import quickSort
from MergeSort import mergeSort
from ParallelMerge import merge_sort

sys.setrecursionlimit(1000000000)

print('Importing Datasets')
start = datetime.now()
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
end = datetime.now()
time = end - start
print('Time Elapsed (Seconds) To Import Data : ',time.total_seconds())

# Sequential Merge Sort
Merge_Sequential = {}
try:
  print('Sequential Merge Sort HundredThousand Begin')
  data = HundredThousand.copy()
  start = datetime.now()
  mergeSort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Sequential['HundredThousand'] = time.total_seconds()
  print('Sequential Merge Sort HundredThousand Done')
except:
  Merge_Sequential['HundredThousand'] = 0
  print('Sequential Merge Sort HundredThousand Failed')
try:
  print('Sequential Merge Sort OneMillion Begin')
  data = OneMillion.copy()
  start = datetime.now()
  mergeSort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Sequential['OneMillion'] = time.total_seconds()
  print('Sequential Merge Sort OneMillion Done')
except:
  Merge_Sequential['OneMillion'] = 0
  print('Sequential Merge Sort OneMillion Failed')
try:
  print('Sequential Merge Sort TenMillion Begin')
  data = TenMillion.copy()
  start = datetime.now()
  mergeSort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Sequential['TenMillion'] = time.total_seconds()
  print('Sequential Merge Sort TenMillion Done')
except:
  Merge_Sequential['TenMillion'] = 0
  print('Sequential Merge Sort TenMillion Failed')
try:
  print('Sequential Merge Sort FiftyMillion Begin')
  data = FiftyMillion.copy()
  start = datetime.now()
  mergeSort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Sequential['FiftyMillion'] = time.total_seconds()
  print('Sequential Merge Sort FiftyMillion Done')
except:
  Merge_Sequential['FiftyMillion'] = 0
  print('Sequential Merge Sort FiftyMillion Failed')
try:
  print('Sequential Merge Sort HundredMillion Begin')
  data = HundredMillion.copy()
  start = datetime.now()
  mergeSort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Sequential['HundredMillion'] = time.total_seconds()
  print('Sequential Merge Sort HundredMillion Done')
except:
  Merge_Sequential['HundredMillion'] = 0
print('Sequential Merge Sort : ',Merge_Sequential,'\n')

# Parallel Merge Sort
Merge_Parallel = {}
try:
  print('Parallel Merge Sort HundredThousand Begin')
  data = HundredThousand.copy()
  start = datetime.now()
  merge_sort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Parallel['HundredThousand'] = time.total_seconds()
  print('Parallel Merge Sort HundredThousand Done')
except:
  Merge_Parallel['HundredThousand'] = 0
  print('Parallel Merge Sort HundredThousand Failed')
try:
  print('Parallel Merge Sort OneMillion Begin')
  data = OneMillion.copy()
  start = datetime.now()
  merge_sort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Parallel['OneMillion'] = time.total_seconds()
  print('Parallel Merge Sort OneMillion Done')
except:
  Merge_Parallel['OneMillion'] = 0
  print('Parallel Merge Sort OneMillion Failed')
try:
  print('Parallel Merge Sort TenMillion Begin')
  data = TenMillion.copy()
  start = datetime.now()
  merge_sort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Parallel['TenMillion'] = time.total_seconds()
  print('Parallel Merge Sort TenMillion Done')
except:
  Merge_Parallel['TenMillion'] = 0
  print('Parallel Merge Sort TenMillion Failed')
try:
  print('Parallel Merge Sort FiftyMillion Begin')
  data = FiftyMillion.copy()
  start = datetime.now()
  merge_sort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Parallel['FiftyMillion'] = time.total_seconds()
  print('Parallel Merge Sort FiftyMillion Done')
except:
  Merge_Parallel['FiftyMillion'] = 0
  print('Parallel Merge Sort FiftyMillion Failed')
try:
  print('Parallel Merge Sort HundredMillion Begin')
  data = HundredMillion.copy()
  start = datetime.now()
  merge_sort(data)
  end = datetime.now()
  time = end - start
  del data
  Merge_Parallel['HundredMillion'] = time.total_seconds()
  print('Parallel Merge Sort HundredMillion Done')
except:
  Merge_Parallel['HundredMillion'] = 0
  print('Parallel Merge Sort HundredMillion Failed')
print('Parallel Merge Sort : ',Merge_Parallel,'\n')

# Sequential Quick Sort
Quick_Sequential = {}
try:
  print('Sequential Quick Sort HundredThousand Begin')
  data = HundredThousand.copy()
  start = datetime.now()
  quickSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Quick_Sequential['HundredThousand'] = time.total_seconds()
  print('Sequential Quick Sort HundredThousand Done')
except:
  Quick_Sequential['HundredThousand'] = 0
  print('Sequential Quick Sort HundredThousand Failed')
try:
  print('Sequential Quick Sort OneMillion Begin')
  data = OneMillion.copy()
  start = datetime.now()
  quickSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Quick_Sequential['OneMillion'] = time.total_seconds()
  print('Sequential Quick Sort OneMillion Done')
except:
  Quick_Sequential['OneMillion'] = 0
  print('Sequential Quick Sort OneMillion Failed')
try:
  print('Sequential Quick Sort TenMillion Begin')
  data = TenMillion.copy()
  start = datetime.now()
  quickSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Quick_Sequential['TenMillion'] = time.total_seconds()
  print('Sequential Quick Sort TenMillion Done')
except:
  Quick_Sequential['TenMillion'] = 0
  print('Sequential Quick Sort TenMillion Failed')
try:
  print('Sequential Quick Sort FiftyMillion Begin')
  data = FiftyMillion.copy()
  start = datetime.now()
  quickSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Quick_Sequential['FiftyMillion'] = time.total_seconds()
  print('Sequential Quick Sort FiftyMillion Done')
except:
  Quick_Sequential['FiftyMillion'] = 0
  print('Sequential Quick Sort FiftyMillion Failed')
try:
  print('Sequential Quick Sort HundredMillion Begin')
  data = HundredMillion.copy()
  start = datetime.now()
  quickSort(data,False,False)
  end = datetime.now()
  time = end - start
  del data
  Quick_Sequential['HundredMillion'] = time.total_seconds()
  print('Sequential Quick Sort HundredMillion Done')
except:
  Quick_Sequential['HundredMillion'] = 0
  print('Sequential Quick Sort HundredMillion Failed')
print('Sequential Quick Sort : ',Quick_Sequential,'\n')

# Parallel Quick Sort
Quick_Parallel = {}
try:
  print('Parallel Quick Sort HundredThousand Begin')
  data = HundredThousand.copy()
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_Parallel['HundredThousand'] = time.total_seconds()
  print('Parallel Quick Sort HundredThousand Done')
except:
  Quick_Parallel['HundredThousand'] = 0
  print('Parallel Quick Sort HundredThousand Failed')
try:
  print('Parallel Quick Sort OneMillion Begin')
  data = OneMillion.copy()
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_Parallel['OneMillion'] = time.total_seconds()
  print('Parallel Quick Sort OneMillion Done')
except:
  Quick_Parallel['OneMillion'] = 0
  print('Parallel Quick Sort OneMillion Failed')
try:
  print('Parallel Quick Sort TenMillion Begin')
  data = TenMillion.copy()
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_Parallel['TenMillion'] = time.total_seconds()
  print('Parallel Quick Sort TenMillion Done')
except:
  Quick_Parallel['TenMillion'] = 0
  print('Parallel Quick Sort TenMillion Failed')
try:
  print('Parallel Quick Sort FiftyMillion Begin')
  data = FiftyMillion.copy()
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_Parallel['FiftyMillion'] = time.total_seconds()
  print('Parallel Quick Sort FiftyMillion Done')
except:
  Quick_Parallel['FiftyMillion'] = 0
  print('Parallel Quick Sort FiftyMillion Failed')
try:
  print('Parallel Quick Sort HundredMillion Begin')
  data = HundredMillion.copy()
  start = datetime.now()
  quickSort(data,True,True)
  end = datetime.now()
  time = end - start
  del data
  Quick_Parallel['HundredMillion'] = time.total_seconds()
  print('Parallel Quick Sort HundredMillion Done')
except:
  Quick_Parallel['HundredMillion'] = 0
  print('Parallel Quick Sort HundredMillion Failed')
print('Parallel Quick Sort : ',Quick_Parallel,'\n')


print('Plotting and Exporting Begin')
x = ['100K','1M','10M','50M','100M']
y0 = [0 for _ in range(len(x))]

Quick_Sequential = [Quick_Sequential['HundredThousand'],Quick_Sequential['OneMillion'],Quick_Sequential['TenMillion'],Quick_Sequential['FiftyMillion'],Quick_Sequential['HundredMillion']]
Quick_Parallel = [Quick_Parallel['HundredThousand'],Quick_Parallel['OneMillion'],Quick_Parallel['TenMillion'],Quick_Parallel['FiftyMillion'],Quick_Parallel['HundredMillion']]
Merge_Sequential = [Merge_Sequential['HundredThousand'],Merge_Sequential['OneMillion'],Merge_Sequential['TenMillion'],Merge_Sequential['FiftyMillion'],Merge_Sequential['HundredMillion']]
Merge_Parallel = [Merge_Parallel['HundredThousand'],Merge_Parallel['OneMillion'],Merge_Parallel['TenMillion'],Merge_Parallel['FiftyMillion'],Merge_Parallel['HundredMillion']]

# Plotting Merge Sort Metrics
plt.plot(x, Merge_Sequential, color='blue', label='Sequential Merge Sort')
plt.plot(x, Merge_Parallel, color='green', label='Parallel Merge Sort')
plt.xlabel('Number of Integers to Sort')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Execution Time of Merge Sort Sorting Algorithms')
plt.savefig('Plots/MergeSort.png', dpi=150)
print('MergeSort.png Exported')
plt.clf()

# Plotting Quick Sort Metrics
plt.plot(x, Quick_Sequential, color='blue', linestyle='dotted', label='Sequential Quick Sort')
plt.plot(x, Quick_Parallel, color='green', linestyle='dotted', label='Parallel Quick Sort')
plt.xlabel('Number of Integers to Sort')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Execution Time of Quick Sort Sorting Algorithms')
plt.savefig('Plots/QuickSort.png', dpi=150)
print('QuickSort.png Exported')
plt.clf()

# Plotting All Metrics In One Graph
plt.plot(x, Quick_Sequential, color='blue', linestyle='dotted', label='Sequential Quick Sort')
plt.plot(x, Quick_Parallel, color='green', linestyle='dotted', label='Parallel Quick Sort')
plt.plot(x, Merge_Sequential, color='blue', label='Sequential Merge Sort')
plt.plot(x, Merge_Parallel, color='green', label='Parallel Merge Sort')
plt.xlabel('Number of Integers to Sort')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Execution Time of All Sorting Algorithms')
plt.savefig('Plots/Sort.png', dpi=150)
print('Sort.png Exported')
plt.clf()