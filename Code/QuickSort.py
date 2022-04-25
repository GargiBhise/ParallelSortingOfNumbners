import concurrent.futures
import sys
import random 

sys.setrecursionlimit(1000000000)

def sub_partition(array, start, end, idx_pivot):
    'returns the position where the pivot winds up'
    if not (start <= idx_pivot <= end):
        raise ValueError('idx pivot must be between start and end')
    array[start], array[idx_pivot] = array[idx_pivot], array[start]
    pivot = array[start]
    i = start + 1
    j = start + 1
    while j <= end:
        if array[j] <= pivot:
            array[j], array[i] = array[i], array[j]
            i += 1
        j += 1
    array[start], array[i - 1] = array[i - 1], array[start]
    return i - 1

def quickSort(array, process, thread, start=0, end=None):
    if end is None:
        end = len(array) - 1
    if end - start < 1:
        return
    idx_pivot = random.randint(start, end)
    i = sub_partition(array, start, end, idx_pivot)
    #print array, i, idx_pivot
    if process == True and thread==True:
      with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(quickSort,array,False,True, start, i - 1)
        executor.submit(quickSort,array,False,True, i + 1, end)
    elif process == False and thread==True:
      with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(quickSort,array,False,False, start, i - 1)
        executor.submit(quickSort,array,False,False, i + 1, end)
    elif process==False and thread==False:
      quickSort(array,False,False, start, i - 1)
      quickSort(array,False,False, i + 1, end)  
