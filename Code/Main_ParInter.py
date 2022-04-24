#!/usr/bin/env python3

import faulthandler
faulthandler.enable()

import sys
import concurrent.futures
import matplotlib.pyplot as plt
from QuickSort import quickSort
from MergeSort import mergeSort
from datetime import datetime
from ImportData import HundredThousand,OneMillion,TenMillion
print('Dataset Import Successful')

sys.setrecursionlimit(20000000)

timelog_quick = {}
timelog_merge = {}
timelog_parallel_quick = {}
timelog_parallel_merge = {}
timelog_parallel = {}
timelog_parallel_quickmerge = {}
timelog_parallel_mergequick = {}

def dummy_parallel(arr,args):
    mergeSort(arr,args)
    return arr

def dummy_sequential(data,algo,size,args):
    try:
        start = datetime.now()
        if algo=='Quick':
            quickSort(data,args)
        elif algo=='Merge':
            mergeSort(data,args)
        end = datetime.now()
        time = end - start
        if algo=='Quick':
            timelog_quick[size] = time.total_seconds()
            print('Sequential Quick {} Done'.format(size))
        elif algo=='Merge':
            timelog_merge[size] = time.total_seconds()
            print('Sequential Merge {} Done'.format(size))
    except:
        if algo=='Quick':
            timelog_quick[size] = -1
            print('Sequential Quick {} Fail'.format(size))
        elif algo=='Merge':
            timelog_merge[size] = -1
            print('Sequential Merge {} Fail'.format(size))


with concurrent.futures.ProcessPoolExecutor() as executor:
    # Parallel Merge Sort (Data Divided and Sorted in Parallel ; Algorithm in Executed Sequentially)
    data = HundredThousand
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_merge['HundredThousand'] = time.total_seconds()
    print('Parallel Merge Sort HundredThousand Done')
    data = OneMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_merge['OneMillion'] = time.total_seconds()
    print('Parallel Merge Sort OneMillion Done')
    data = TenMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    print('Parallel Merge Sort TenMillion Done')

    # Parallel Quick Sort (Data Divided and Sorted in Parallel ; Algorithm in Executed Sequentially)
    data = HundredThousand
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quick['HundredThousand'] = time.total_seconds()
    print('Parallel Quick Sort HundredThousand Done')
    data = OneMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quick['OneMillion'] = time.total_seconds()
    print('Parallel Quick Sort OneMillion Done')
    data = TenMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quick['TenMillion'] = time.total_seconds()
    print('Parallel Quick Sort TenMillion Done')

    # Sequential Quick Sort
    data = HundredThousand
    executor.submit(dummy_sequential,data,'Quick','HundredThousand',True)
    del data
    data = OneMillion
    executor.submit(dummy_sequential,data,'Quick','OneMillion',True)
    del data
    data = TenMillion
    executor.submit(dummy_sequential,data,'Quick','TenMillion',True)
    del data

    # Sequential Merge Sort
    data = HundredThousand
    executor.submit(dummy_sequential,data,'Merge','HundredThousand',True)
    del data
    data = OneMillion
    executor.submit(dummy_sequential,data,'Merge','OneMillion',True)
    del data
    data = TenMillion
    executor.submit(dummy_sequential,data,'Merge','TenMillion',True)
    del data

    # Parallel Quick+Merge Sort 
    # Data Divided and Sorted in Parallel using Quick Sort; Chunks Merged Using Merge Sort ; Algorithm in Executed Sequentially
    data = HundredThousand
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quickmerge['HundredThousand'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort HundredThousand Done')
    data = OneMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quickmerge['OneMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort OneMillion Done')
    data = TenMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    mergeSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_quickmerge['TenMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort TenMillion Done')

    # Parallel Merge+Quick Sort 
    # Data Divided and Sorted in Parallel using Merge Sort; Chunks Merged Using Quick Sort ; Algorithm in Executed Sequentially
    data = HundredThousand
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_mergequick['HundredThousand'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort HundredThousand Done')
    data = OneMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_mergequick['OneMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort OneMillion Done')
    data = TenMillion
    final_data = []
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',True)
    chunk1 = list(data1.result())
    final_data.extend(chunk1)
    del chunk1
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',True)
    chunk2 = list(data2.result())
    final_data.extend(chunk2)
    del chunk2
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',True)
    chunk3 = list(data3.result())
    final_data.extend(chunk3)
    del chunk3
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',True)
    chunk4 = list(data4.result())
    final_data.extend(chunk4)
    del chunk4
    quickSort(final_data,True)
    end = datetime.now()
    del final_data
    time = end - start
    timelog_parallel_mergequick['TenMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort TenMillion Done')

x = ['Hundred Thousand','One Million','Ten Million']
y0 = [0 for _ in range(3)]
merge = [timelog_merge['HundredThousand'],timelog_merge['OneMillion'],timelog_merge['TenMillion']]
quick = [timelog_quick['HundredThousand'],timelog_quick['OneMillion'],timelog_quick['TenMillion']]
parallel_quick = [timelog_parallel_quick['HundredThousand'],timelog_parallel_quick['OneMillion'],timelog_parallel_quick['TenMillion']]
parallel_merge = [timelog_parallel_merge['HundredThousand'],timelog_parallel_merge['OneMillion'],timelog_parallel_merge['TenMillion']]
parallel_quickmerge = [timelog_parallel_quickmerge['HundredThousand'],timelog_parallel_quickmerge['OneMillion'],timelog_parallel_quickmerge['TenMillion']]
parallel_mergequick = [timelog_parallel_mergequick['HundredThousand'],timelog_parallel_mergequick['OneMillion'],timelog_parallel_mergequick['TenMillion']]
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quick, color='red', label='Parallel Quick Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelQuick_ParInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_merge, color='red', label='Parallel Merge Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMerge_ParInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Quick(Chunks)+Merge(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelQuickMerge_ParInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Merge(Chunks)+Quick(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMergeQuick_ParInter.png', dpi=150)
plt.clf()
plt.plot(x, parallel_quick, color='black', label='Parallel Quick Sort')
plt.plot(x, parallel_merge, color='red', label='Parallel Merge Sort')
plt.plot(x, parallel_quickmerge, color='green', label='Parallel Quick(Chunks)+Merge(Combined) Sort')
plt.plot(x, parallel_quickmerge, color='blue', label='Parallel Merge(Chunks)+Quick(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelAll_ParInter.png', dpi=150)
plt.clf()
print('Graph Plotting Complete')
