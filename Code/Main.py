import sys
import concurrent.futures
import matplotlib.pyplot as plt
from QuickSort import quickSort
from MergeSort import mergeSort
from datetime import datetime
from ImportData import HundredThousand,OneMillion,TenMillion,HundredMillion
print('Dataset Import Successful')

sys.setrecursionlimit(1000000000)

timelog_quick = {}
timelog_merge = {}
timelog_parallel_quick = {}
timelog_parallel_merge = {}
timelog_parallel = {}
timelog_parallel_quickmerge = {}
timelog_parallel_mergequick = {}

def dummy_parallel(arr,algo):
    if algo=='Quick':
        quickSort(arr,0,len(arr)-1)
        return arr
    elif algo=='Merge':
        mergeSort(arr)
        return arr

def dummy_sequential(data,algo,size):
    try:
        start = datetime.now()
        if algo=='Quick':
            quickSort(data,0,len(data)-1)
        elif algo=='Merge':
            mergeSort(data)
        end = datetime.now()
        time = end - start
        if algo=='Quick':
            timelog_quick[size] = time.total_seconds()
            print('Sequential Quick {} Done'.format(len(data)))
        elif algo=='Merge':
            timelog_merge[size] = time.total_seconds()
            print('Sequential Merge {} Done'.format(len(data)))
    except:
        if algo=='Quick':
            timelog_quick[size] = -1
            print('Sequential Quick {} Fail'.format(len(data)))
        elif algo=='Merge':
            timelog_merge[size] = -1
            print('Sequential Merge {} Fail'.format(len(data)))

with concurrent.futures.ProcessPoolExecutor() as executor:

    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['HundredThousand'] = time.total_seconds()
    print('Parallel Merge Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['OneMillion'] = time.total_seconds()
    print('Parallel Merge Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['TenMillion'] = time.total_seconds()
    print('Parallel Merge Sort TenMillion Done')
    data = HundredMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_merge['HundredMillion'] = time.total_seconds()
    print('Parallel Merge Sort HundredMillion Done')

    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['HundredThousand'] = time.total_seconds()
    print('Parallel Quick Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['OneMillion'] = time.total_seconds()
    print('Parallel Quick Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['TenMillion'] = time.total_seconds()
    print('Parallel Quick Sort TenMillion Done')
    data = HundredMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_quick['HundredMillion'] = time.total_seconds()
    print('Parallel Quick Sort HundredMillion Done')

    data = HundredThousand
    executor.submit(dummy_sequential,data,'Quick','HundredThousand')
    data = OneMillion
    executor.submit(dummy_sequential,data,'Quick','OneMillion')
    data = TenMillion
    executor.submit(dummy_sequential,data,'Quick','TenMillion')
    data = HundredMillion
    executor.submit(dummy_sequential,data,'Quick','HundredMillion')

    data = HundredThousand
    executor.submit(dummy_sequential,data,'Merge','HundredThousand')
    data = OneMillion
    executor.submit(dummy_sequential,data,'Merge','OneMillion')
    data = TenMillion
    executor.submit(dummy_sequential,data,'Merge','TenMillion')
    data = HundredMillion
    executor.submit(dummy_sequential,data,'Merge','HundredMillion')

    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['HundredThousand'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['OneMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['TenMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort TenMillion Done')
    data = HundredMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Quick')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Quick')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Quick')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Quick')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)
    end = datetime.now()
    time = end - start
    timelog_parallel_quickmerge['HundredMillion'] = time.total_seconds()
    print('Parallel Quick(Chunks)+Merge(Combined) Sort HundredMillion Done')

    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['HundredThousand'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort HundredThousand Done')
    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['OneMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort OneMillion Done')
    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['TenMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort TenMillion Done')
    data = HundredMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)],'Merge')
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)],'Merge')
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)],'Merge')
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):],'Merge')
    chunk4 = list(data4.result())
    final_data = chunk1+chunk2+chunk3+chunk4
    quickSort(final_data,0,len(final_data)-1)
    end = datetime.now()
    time = end - start
    timelog_parallel_mergequick['HundredMillion'] = time.total_seconds()
    print('Parallel Merge(Chunks)+Quick(Combined) Sort HundredMillion Done')

x = ['Hundred Thousand','One Million','Ten Million','Hundred Million']
y0 = [0 for _ in range(4)]
merge = [timelog_merge['HundredThousand'],timelog_merge['OneMillion'],timelog_merge['TenMillion'],timelog_merge['HundredMillion']]
quick = [timelog_quick['HundredThousand'],timelog_quick['OneMillion'],timelog_quick['TenMillion'],timelog_quick['HundredMillion']]
parallel_quick = [timelog_parallel_quick['HundredThousand'],timelog_parallel_quick['OneMillion'],timelog_parallel_quick['TenMillion'],timelog_parallel_quick['HundredMillion']]
parallel_merge = [timelog_parallel_merge['HundredThousand'],timelog_parallel_merge['OneMillion'],timelog_parallel_merge['TenMillion'],timelog_parallel_merge['HundredMillion']]
parallel_quickmerge = [timelog_parallel_quickmerge['HundredThousand'],timelog_parallel_quickmerge['OneMillion'],timelog_parallel_quickmerge['TenMillion'],timelog_parallel_quickmerge['HundredMillion']]
parallel_mergequick = [timelog_parallel_mergequick['HundredThousand'],timelog_parallel_mergequick['OneMillion'],timelog_parallel_mergequick['TenMillion'],timelog_parallel_mergequick['HundredMillion']]
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quick, color='red', label='Parallel Quick Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelQuick.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_merge, color='red', label='Parallel Merge Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMerge.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Quick(Chunks)+Merge(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelQuickMerge.png', dpi=150)
plt.clf()
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel_quickmerge, color='red', label='Parallel Merge(Chunks)+Quick(Combined) Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/ParallelMergeQuick.png', dpi=150)
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
plt.savefig('../Plots/ParallelAll.png', dpi=150)
plt.clf()
print('Graph Plotting Complete')
