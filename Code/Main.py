import csv
import sys
import matplotlib.pyplot as plt
from datetime import datetime
from QuickSort import quickSort
from MergeSort import mergeSort
from ParallelSort import parallel_sort

sys.setrecursionlimit(10000)

final_data = []
timelog_parallel = []
timelog_merge = []
timelog_quick = []

def sort_log(filename):

    with open('../Datasets/'+filename, newline='\n') as f:
        reader = csv.reader(f)
        data = list(reader)
        data = [int(i[0]) for i in data]
        time_log['Data Size'].append(len(data))
    print('\n',filename,' Imported')

    # Parallel Sorting
    start = datetime.now()
    if parallel_sort(data):
        end = datetime.now()
        time = end - start
        timelog_parallel.append(time.total_seconds())
    else:
        timelog_parallel.append(-1)
    print('Completed Parallel Sorting for ',filename)

    # Sequential Merge Sort
    start = datetime.now()
    final_data = data
    try:
        mergeSort(final_data)
        end = datetime.now()
        time = end - start
        timelog_merge.append(time.total_seconds())
    except:
        timelog_merge.append(-1)
    print('Completed Sequential Merge Sort for ',filename)

    # Sequential Quick Sort
    start = datetime.now()
    final_data = data
    try:
        quickSort(final_data,0,len(final_data)-1)
        end = datetime.now()
        time = end - start
        timelog_quick.append(time.total_seconds())
    except:
        timelog_quick.append(-1)
    print('Completed Sequential Quick Sort for ',filename)


datasets = ['HundredThousand.csv','OneMillion.csv','TenMillion.csv','HundredMillion.csv']

for files in datasets:
    sort_log(files)

# Plotting Time Logs for Each Step
x = ['Hundred Thousand','One Million','Ten Million','Hundred Million']
y0 = [0 for _ in range(len(datasets))]
plt.plot(x, y0, color='black', linestyle='--', label='Zero Line')
plt.plot(x, timelog_parallel, color='red', label='Parallel Sort')
plt.plot(x, timelog_merge, color='green', label='Sequential Merge Sort')
plt.plot(x, timelog_quick, color='blue', label='Sequential Quick Sort')
plt.title('Total Execution Time of Each Step v/s Size of Data')
plt.xlabel('Size of Dataset')
plt.ylabel('Time Taken in Seconds')
plt.legend()
plt.savefig('../Plots/TimeLog.png', dpi=150)
print('Graph Plotting Complete')