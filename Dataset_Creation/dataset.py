#!/usr/bin/env python
# coding: utf-8

import csv;
import random;

#Generate People, Infected-small, and infected-large

with open('PEOPLE.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)

    for i in range(1,1000001):
        id = i
        x = random.randrange(0,10001)
        y = random.randrange(0,10001)
        data = [id,x,y]
        writer.writerow(data)

    print("done");

with open('INFECTED-SMALL.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)

    for i in range(1,10001):
        id = i
        x = random.randrange(0,10001)
        y = random.randrange(0,10001)
        data = [id,x,y]
        writer.writerow(data)

    print("done");

with open('INFECTED-LARGE.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)

    for i in range(1,1000001):
        id = i
        x = random.randrange(0,10001)
        y = random.randrange(0,10001)
        data = [id,x,y]
        writer.writerow(data)

    print("done");





