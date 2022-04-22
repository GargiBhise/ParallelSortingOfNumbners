#Importing Required Libraries
import random
import os
import psutil
import matplotlib.pyplot as plt
from datetime import datetime

# Declaring Key Variables
cpu_usage = []
ram_usage = []
time_log = {}

# Dataset Generation Start Time
start = datetime.now()
# Getting One Billion Numbers from 1 to 1,000,000,000
data = []
for i in range(1,1000000000+1):
  cpu_usage.append(psutil.cpu_percent()) # Logging CPU Usage
  data.append(str(i))
  ramusage = (psutil.virtual_memory()[5]/psutil.virtual_memory()[0])*100 # Logging RAM Usage
  ram_usage.append(round(ramusage,3)) 
# Dataset Generation End Time
end = datetime.now()
# Time (In Seconds) Needed to Generate Dataset
total_time = end - start
time_log['Dataset Generation'] = total_time.total_seconds()

# Plotting RAM Usage % v/s CPU % Usage Plot for Analysis
plt.plot(cpu_usage,ram_usage)
plt.xlabel('CPU Usage %')
plt.ylabel('RAM Usage %')
plt.savefig('Plots/DatasetGeneration_RAMvsCPU.png', dpi=150)
plt.clf()

# Dataset Shuffle Start Time
start = datetime.now()
# Randomly Shuffling the Dataset
random.shuffle(data)
# Dataset Shuffle End Time
end = datetime.now()
# Time (In Seconds) Needed to Shuffle Dataset
total_time = end - start
time_log['Dataset Shuffle'] = total_time.total_seconds()

# Data To CSV Start Time
start = datetime.now()
# Saving the Generated Data in a CSV File
data = '\n'.join(data)
f = open('Data.csv','w')
f.write(data)
f.close()
# Data To CSV End Time
end = datetime.now()
# Time (In Seconds) Needed to Save Data to CSV 
total_time = end - start
time_log['Dataset To CSV'] = total_time.total_seconds()

# Plotting Time Taken For Each Step for Analysis
x = list(time_log.keys())
y = list(time_log.values())
plt.bar(range(len(time_log)), height=y, tick_label=x)
plt.ylabel('Time Taken in Seconds')
plt.savefig('Plots/DatasetGenerationAndExporting_TimeLog.png', dpi=150)
plt.clf()