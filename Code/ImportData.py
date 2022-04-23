#!/usr/bin/env python3

import csv

with open('../Datasets/HundredThousand.csv', newline='\n') as f:
    reader = csv.reader(f)
    HundredThousand = list(reader)
    HundredThousand = [int(i[0]) for i in HundredThousand]

with open('../Datasets/OneMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    OneMillion = list(reader)
    OneMillion = [int(i[0]) for i in OneMillion]

with open('../Datasets/TenMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    TenMillion = list(reader)
    TenMillion = [int(i[0]) for i in TenMillion]

with open('../Datasets/HundredMillion.csv', newline='\n') as f:
    reader = csv.reader(f)
    HundredMillion = list(reader)
    HundredMillion = [int(i[0]) for i in HundredMillion]
