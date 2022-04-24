import sys
from datetime import datetime
import concurrent.futures

from ImportData import read_data
from QuickSort import quickSort
from ErrorString import errorstr
print(errorstr)
# from MergeSort import mergeSort

sys.setrecursionlimit(1000000000)
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
