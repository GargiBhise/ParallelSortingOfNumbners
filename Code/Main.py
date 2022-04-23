import sys
import concurrent.futures
import matplotlib.pyplot as plt
from QuickSort import quickSort
from MergeSort import mergeSort
from datetime import datetime
from ImportData import HundredThousand,OneMillion,TenMillion,HundredMillion
print('Dataset Import Successful')

sys.setrecursionlimit(10000)

timelog_quick = {}
timelog_merge = {}
timelog_parallel = {}

def dummy_parallel(arr):
	quickSort(arr,0,len(arr)-1)
	return arr

def dummy_sequential(data,algo):
    try:
        start = datetime.now()
        if algo=='Quick':
            quickSort(data,0,len(data)-1)
        elif algo=='Merge':
            mergeSort(data)
        end = datetime.now()
        time = end - start
        if algo=='Quick':
            timelog_quick[len(data)] = time.total_seconds()
            print('Sequential Quick {} Done'.format(len(data)))
        elif algo=='Merge':
            timelog_merge[len(data)] = time.total_seconds()
            print('Sequential Merge {} Done'.format(len(data)))
    except:
        if algo=='Quick':
            timelog_quick[len(data)] = -1
            print('Sequential Quick {} Fail'.format(len(data)))
        elif algo=='Merge':
            timelog_merge[len(data)] = -1
            print('Sequential Merge {} Fail'.format(len(data)))

with concurrent.futures.ProcessPoolExecutor() as executor:

    data = HundredThousand
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)])
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)])
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)])
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):])
    chunk4 = list(data4.result())
    data5 = executor.submit(dummy_parallel,chunk1+chunk2)
    chunk5 = list(data5.result())
    data6 = executor.submit(dummy_parallel,chunk3+chunk4)
    chunk6 = list(data6.result())
    final_data = chunk5+chunk6
    mergeSort(final_data)
    end = datetime.now()
    if final_data==sorted(data):
        print('Parallel Sort {} Done'.format(len(data)))
        time = end - start
        timelog_parallel[len(data)] = time.total_seconds()
    else:
        timelog_parallel[len(data)] = -1
        print('Parallel Sort {} Fail'.format(len(data)))

    data = OneMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)])
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)])
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)])
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):])
    chunk4 = list(data4.result())
    data5 = executor.submit(dummy_parallel,chunk1+chunk2)
    chunk5 = list(data5.result())
    data6 = executor.submit(dummy_parallel,chunk3+chunk4)
    chunk6 = list(data6.result())
    final_data = chunk5+chunk6
    mergeSort(final_data)
    end = datetime.now()
    if final_data==sorted(data):
        print('Parallel Sort {} Done'.format(len(data)))
        time = end - start
        timelog_parallel[len(data)] = time.total_seconds()
    else:
        timelog_parallel[len(data)] = -1
        print('Parallel Sort {} Fail'.format(len(data)))

    data = TenMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)])
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)])
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)])
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):])
    chunk4 = list(data4.result())
    data5 = executor.submit(dummy_parallel,chunk1+chunk2)
    chunk5 = list(data5.result())
    data6 = executor.submit(dummy_parallel,chunk3+chunk4)
    chunk6 = list(data6.result())
    final_data = chunk5+chunk6
    mergeSort(final_data)
    end = datetime.now()
    if final_data==sorted(data):
        print('Parallel Sort {} Done'.format(len(data)))
        time = end - start
        timelog_parallel[len(data)] = time.total_seconds()
    else:
        timelog_parallel[len(data)] = -1
        print('Parallel Sort {} Fail'.format(len(data)))

    data = HundredMillion
    start = datetime.now()
    data1 = executor.submit(dummy_parallel,data[:int(len(data)*0.25)])
    chunk1 = list(data1.result())
    data2 = executor.submit(dummy_parallel,data[int(len(data)*0.25):int(len(data)*0.5)])
    chunk2 = list(data2.result())
    data3 = executor.submit(dummy_parallel,data[int(len(data)*0.5):int(len(data)*0.75)])
    chunk3 = list(data3.result())
    data4 = executor.submit(dummy_parallel,data[int(len(data)*0.75):])
    chunk4 = list(data4.result())
    data5 = executor.submit(dummy_parallel,chunk1+chunk2)
    chunk5 = list(data5.result())
    data6 = executor.submit(dummy_parallel,chunk3+chunk4)
    chunk6 = list(data6.result())
    final_data = chunk5+chunk6
    mergeSort(final_data)
    end = datetime.now()
    if final_data==sorted(data):
        print('Parallel Sort {} Done'.format(len(data)))
        time = end - start
        timelog_parallel[len(data)] = time.total_seconds()
    else:
        timelog_parallel[len(data)] = -1
        print('Parallel Sort {} Fail'.format(len(data)))

    data = HundredThousand
    executor.submit(dummy_sequential,data,'Quick')
    data = OneMillion
    executor.submit(dummy_sequential,data,'Quick')
    data = TenMillion
    executor.submit(dummy_sequential,data,'Quick')
    data = HundredMillion
    executor.submit(dummy_sequential,data,'Quick')

    data = HundredThousand
    executor.submit(dummy_sequential,data,'Merge')
    data = OneMillion
    executor.submit(dummy_sequential,data,'Merge')
    data = TenMillion
    executor.submit(dummy_sequential,data,'Merge')
    data = HundredMillion
    executor.submit(dummy_sequential,data,'Merge')


x = ['Hundred Thousand','One Million','Ten Million','Hundred Million']
y0 = [0 for _ in range(4)]
merge = [timelog_merge[100000],timelog_merge[1000000],timelog_merge[10000000],timelog_merge[100000000]]
quick = [timelog_quick[100000],timelog_quick[1000000],timelog_quick[10000000],timelog_quick[100000000]]
parallel = [timelog_parallel[100000],timelog_parallel[1000000],timelog_parallel[10000000],timelog_parallel[100000000]]
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, parallel, color='red', label='Parallel Sort')
plt.plot(x, merge, color='green', label='Sequential Merge Sort')
plt.plot(x, quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/TimeLog.png', dpi=150)
print('Graph Plotting Complete')
