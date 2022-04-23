import csv
import sys
import matplotlib.pyplot as plt
from datetime import datetime
from QuickSort import quickSort
from MergeSort import mergeSort
from ParallelSort import parallel_sort

sys.setrecursionlimit(1000000)

final_data = []
time_log = {'Data Size':[],'Parallel Quick+Merge':[],'Sequential Merge':[],'Sequential Quick':[]}

def sort_log(filename):

    with open('../Datasets/'+filename, newline='\n') as f:
        reader = csv.reader(f)
        data = list(reader)
        data = [int(i[0]) for i in data]
        time_log['Data Size'].append(len(data))
    print(filename,' Imported')

    # Parallel Sorting
    print('Begin Parallel Sorting for ',filename)
    start = datetime.now()
    if parallel_sort():
        end = datetime.now()
        time = end - start
        parallel_sort_time = time.total_seconds()
        time_log['Parallel Quick+Merge'].append(parallel_sort_time)
    else:
        time_log['Parallel Quick+Merge'].append(None)
    print('End Parallel Sorting for ',filename)

    # Sequential Merge Sort
    print('Begin Sequential Merge Sort for ',filename)
    start = datetime.now()
    final_data = data
    try:
        mergeSort(final_data)
        end = datetime.now()
        time = end - start
        sequential_sort_time = time.total_seconds()
        time_log['Sequential Merge'] = sequential_sort_time
    except:
        time_log['Sequential Merge'] = None
    print('End Sequential Merge Sort for ',filename)

    # Sequential Quick Sort
    print('Begin Sequential Quick Sort for ',filename)
    start = datetime.now()
    final_data = data
    try:
        quickSort(final_data,0,len(final_data)-1)
        end = datetime.now()
        time = end - start
        sequential_sort_time = time.total_seconds()
        time_log['Sequential Quick'] = sequential_sort_time
    except:
        time_log['Sequential Quick'] = None
    print('End Sequential Quick Sort for ',filename)

# Plotting Time Logs for Each Step
print('Begin Graph Plotting')
x = list(time_log.keys())
y = list(time_log.values())
plt.bar(range(len(time_log)), height=y, tick_label=x)
plt.ylabel('Time Taken in Seconds')
plt.title('Total Execution Time of Each Step')
plt.savefig('Plots/TimeLog.png', dpi=150)
print('End Graph Plotting')

datasets = ['Thousand.csv','HundredThousand.csv','OneMillion.csv','TenMillion.csv','HundredMillion.csv']

# datasets = ['Thousand.csv','HundredThousand.csv','OneMillion.csv','TenMillion.csv','HundredMillion.csv','FiveHundredMillion.csv','OneBillion.csv']

for files in datasets:
    sort_log(files)

