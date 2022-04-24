import sys
from datetime import datetime
import matplotlib.pyplot as plt
import concurrent.futures

from ImportData import read_data
from QuickSort import quickSort
from MergeSort import mergeSort

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

print('''\nSPST Quick Sort Failed due to Segmentation fault (core dumped)\nSPMT Quick Sort Failed due to Segmentation fault (core dumped)\n\n''')

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

Quick_MPMT = [Quick_MPMT['HundredThousand'],Quick_MPMT['OneMillion'],Quick_MPMT['TenMillion'],Quick_MPMT['FiftyMillion'],Quick_MPMT['HundredMillion']]
Merge_SPST = [Merge_SPST['HundredThousand'],Merge_SPST['OneMillion'],Merge_SPST['TenMillion'],Merge_SPST['FiftyMillion'],Merge_SPST['HundredMillion']]
Merge_SPMT = [Merge_SPMT['HundredThousand'],Merge_SPMT['OneMillion'],Merge_SPMT['TenMillion'],Merge_SPMT['FiftyMillion'],Merge_SPMT['HundredMillion']]
Merge_MPMT = [Merge_MPMT['HundredThousand'],Merge_MPMT['OneMillion'],Merge_MPMT['TenMillion'],Merge_MPMT['FiftyMillion'],Merge_MPMT['HundredMillion']]

plt.plot(x, y0, color='red', linestyle='dotted', label='Single-Process Single-Thread Quick Sort')
plt.plot(x, y0, color='blue', linestyle='dotted', label='Single-Process Multi-Thread Quick Sort')
plt.plot(x, Quick_MPMT, color='green', linestyle='dotted', label='Multi-Process Multi-Thread Quick Sort')
plt.plot(x, Merge_SPST, color='red', linestyle='dashed', label='Single-Process Single-Thread Merge Sort')
plt.plot(x, Merge_SPMT, color='blue', linestyle='dashed', label='Single-Process Multi-Thread Merge Sort')
plt.plot(x, Merge_MPMT, color='green', linestyle='dashed', label='Multi-Process Multi-Thread Merge Sort')
plt.xlabel('Number of Integers to Sort')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.title('Execution Time of Sorting Algorithms')
plt.savefig('../Plots/Sort.png', dpi=150)
print('Sort.png Exported')
