import csv
import random
import string

ROWS = 5000000
descriptions = ["Home Furningish",
                "Business Expenses and Supplies",
                "Child School Supplies",
                "Groceries and Toiletries",
                "Clothes and Shoes",
                ]

with open('Purchases.csv', 'w', newline='') as csvfile:
    csvfile.truncate(0)
    writer = csv.writer(csvfile, delimiter=',')
    # writer.writerow(['TransID', 'CustID', 'TransTotal', 'TransNumItems', 'TransDesc'])
    for i in range(ROWS):
        writer.writerow([
            i + 1,
            random.randint(1, 50000), 
            round(random.uniform(10, 2000),2),
            random.randint(1, 15),
            descriptions[random.randint(0, 4)]
        ])