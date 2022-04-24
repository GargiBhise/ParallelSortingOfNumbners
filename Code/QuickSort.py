import concurrent.futures

def quickSort(arr,process,thread):
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
    if process == True and thread == True:
      with concurrent.futures.ProcessPoolExecutor() as executor:
        p1 = executor.submit(quickSort,arr[0:current_position],False,True) #Sorts the elements to the left of pivot
        p2 = executor.submit(quickSort,arr[current_position+1:elements],False,True) #sorts the elements to the right of pivot
        left = p1.result()
        right = p2.result()
    elif process == False and thread == True:
      with concurrent.futures.ThreadPoolExecutor() as executor:
        p1 = executor.submit(quickSort,arr[0:current_position],False,False) #Sorts the elements to the left of pivot
        p2 = executor.submit(quickSort,arr[current_position+1:elements],False,False) #sorts the elements to the right of pivot
        left = p1.result()
        right = p2.result()
    elif process == False and thread == False:
      left = quickSort(arr[0:current_position],False,False) #Sorts the elements to the left of pivot
      right = quickSort(arr[current_position+1:elements],False,False) #sorts the elements to the right of pivot
    arr = left + [arr[current_position]] + right #Merging everything together
    return arr