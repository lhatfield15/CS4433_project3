import csv
import random
import string

ROWS = 50000
letters = string.ascii_lowercase
with open('Customers.csv', 'w', newline='') as csvfile:
    csvfile.truncate(0)
    writer = csv.writer(csvfile, delimiter=',')
    # writer.writerow(['ID', 'Name', 'Age' ,'CountryCode', 'Salary'])
    for i in range(ROWS):
        writer.writerow([
            i + 1,
            "".join(random.choice(letters) for i in range(10,20)),# names.get_full_name(),
            random.randint(18, 100),
            random.randint(1, 500),  
            random.uniform(100, 10000000)
        ])