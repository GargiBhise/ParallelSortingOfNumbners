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
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['HundredThousand'] = time.total_seconds()
    print('Parallel Merge Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['OneMillion'] = time.total_seconds()
    print('Parallel Merge Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['TenMillion'] = time.total_seconds()
    print('Parallel Merge Sort TenMillion Done')

    # Parallel Quick Sort (Data Divided and Sorted in Parallel ; Algorithm in Executed Sequentially)
    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['HundredThousand'] = time.total_seconds()
    print('Parallel Quick Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['OneMillion'] = time.total_seconds()
    print('Parallel Quick Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['TenMillion'] = time.total_seconds()
    print('Parallel Quick Sort TenMillion Done')

    # Sequential Quick Sort
    data = HundredThousand
    executor.submit(dummy_sequential,data,'Quick','HundredThousand',False)
    data = OneMillion
    executor.submit(dummy_sequential,data,'Quick','OneMillion',False)
    data = TenMillion
    executor.submit(dummy_sequential,data,'Quick','TenMillion',False)

    # Sequential Merge Sort
    data = HundredThousand
    executor.submit(dummy_sequential,data,'Merge','HundredThousand',False)
    data = OneMillion
    executor.submit(dummy_sequential,data,'Merge','OneMillion',False)
    data = TenMillion
    executor.submit(dummy_sequential,data,'Merge','TenMillion',False)

    # Parallel Quick+Merge Sort 
    # Data Divided and Sorted in Parallel using Quick Sort; Chunks Merged Using Merge Sort ; Algorithm in Executed Sequentially
    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['HundredThousand'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['OneMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(quickSort,data[:int(len(data)*0.25)],False)
    chunk1 = list(data1.result())
    data2 = executor.submit(quickSort,data[int(len(data)*0.25):int(len(data)*0.5)],False)
    chunk2 = list(data2.result())
    data3 = executor.submit(quickSort,data[int(len(data)*0.5):int(len(data)*0.75)],False)
    chunk3 = list(data3.result())
    data4 = executor.submit(quickSort,data[int(len(data)*0.75):],False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['TenMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort TenMillion Done')

    # Parallel Merge+Quick Sort 
    # Data Divided and Sorted in Parallel using Merge Sort; Chunks Merged Using Quick Sort ; Algorithm in Executed Sequentially
    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['HundredThousand'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['OneMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge',False)
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge',False)
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge',False)
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge',False)
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,False)
    end = datetime.now()
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
plt.savefig('../Plots/ParallelQuick_SecInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_merge, color='red', label='Parallel Merge Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMerge_SecInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Quick(Chunks)+Merge(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelQuickMerge_SecInter.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Merge(Chunks)+Quick(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMergeQuick_SecInter.png', dpi=150)
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
plt.savefig('../Plots/ParallelAll_SecInter.png', dpi=150)
plt.clf()
print('Graph Plotting Complete')
