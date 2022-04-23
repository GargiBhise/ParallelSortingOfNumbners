import concurrent.futures
from QuickSort import quickSort
from MergeSort import mergeSort

def dummy(arr,algo):
	if algo=='QuickSort':
		quickSort(arr,0,len(arr)-1)
		return arr
	elif algo=='MergeSort':
		mergeSort(arr)
		return arr

def parallel_sort(data):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        
        data1 = executor.submit(dummy,data[:int(len(data)*0.25)],'QuickSort')
        chunk1 = list(data1.result())

        data2 = executor.submit(dummy,data[int(len(data)*0.25):int(len(data)*0.5)],'QuickSort')
        chunk2 = list(data2.result())

        data3 = executor.submit(dummy,data[int(len(data)*0.5):int(len(data)*0.75)],'QuickSort')
        chunk3 = list(data3.result())

        data4 = executor.submit(dummy,data[int(len(data)*0.75):],'QuickSort')
        chunk4 = list(data4.result())

    # with concurrent.futures.ProcessPoolExecutor() as executor:

    #   data5 = executor.submit(dummy,chunk1+chunk2,'MergeSort')
    #   chunk5 = list(data5.result())
    
    #   data6 = executor.submit(dummy,chunk3+chunk4,'MergeSort')
    #   chunk6 = list(data6.result())


    final_data = chunk1+chunk2+chunk3+chunk4
    mergeSort(final_data)

    if final_data==sorted(data):
        return True
    else:
        return False
