#!/usr/bin/env python3

import sys
import concurrent.futures
sys.setrecursionlimit(20000000)

parallel = False
def quickSort(arr,parallel):

    if parallel == False:
        elements = len(arr)
        #Base case
        if elements < 2:
            return arr
        current_position = 0 #Position of the partitioning element
        for i in range(1, elements): #Partitioning loop
            if arr[i] <= arr[0]:
                current_position += 1
                temp = arr[i]
                arr[i] = arr[current_position]
                arr[current_position] = temp
        temp = arr[0]
        arr[0] = arr[current_position] 
        arr[current_position] = temp #Brings pivot to it's appropriate position
        left = quickSort(arr[0:current_position],False) #Sorts the elements to the left of pivot
        right = quickSort(arr[current_position+1:elements],False) #sorts the elements to the right of pivot
        arr = left + [arr[current_position]] + right #Merging everything together
        return arr

    elif parallel == True:
        elements = len(arr)
        #Base case
        if elements < 2:
            return arr
        current_position = 0 #Position of the partitioning element
        for i in range(1, elements): #Partitioning loop
            if arr[i] <= arr[0]:
                current_position += 1
                temp = arr[i]
                arr[i] = arr[current_position]
                arr[current_position] = temp
        temp = arr[0]
        arr[0] = arr[current_position] 
        arr[current_position] = temp #Brings pivot to it's appropriate position
        with concurrent.futures.ProcessPoolExecutor() as executor:
            left = executor.submit(quickSort,arr[0:current_position],True) #Sorts the elements to the left of pivot
            right = executor.submit(quickSort,arr[current_position+1:elements],True) #sorts the elements to the right of pivot
        arr = left + [arr[current_position]] + right #Merging everything together
        return arr
