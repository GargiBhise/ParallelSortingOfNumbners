import sys
import concurrent.futures

sys.setrecursionlimit(1000000000)

def mergeSort(myList,process,thread):
    if len(myList) > 1:
        mid = len(myList) // 2
        left = myList[:mid]
        right = myList[mid:]
        if process == True and thread==True:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.submit(mergeSort,left,False,True)
                executor.submit(mergeSort,right,False,True)
        elif process == False and thread==True:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(mergeSort,left,False,False)
                executor.submit(mergeSort,right,False,False)
        elif process == False and thread==False:
            mergeSort(left,False,False)
            mergeSort(right,False,False)
        # Two iterators for traversing the two halves
        i = 0
        j = 0
        # Iterator for the main list
        k = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
              # The value from the left half has been used
              myList[k] = left[i]
              # Move the iterator forward
              i += 1
            else:
                myList[k] = right[j]
                j += 1
            # Move to the next slot
            k += 1
        # For all the remaining values
        while i < len(left):
            myList[k] = left[i]
            i += 1
            k += 1
        while j < len(right):
            myList[k]=right[j]
            j += 1
            k += 1