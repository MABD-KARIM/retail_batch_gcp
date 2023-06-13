from faker import Faker
fake=Faker()
cities = ["Paris", "Nantes","Lyon","Lille","Marseille","Bordeaux","Toulouse","Rennes","Rouen","Paris","Paris","Paris","Lille","Lille","Lyon","Lyon","Paris"]
import csv
from random import choice
header = ["CustId","CustFirstName","CustLastName","CustCity"]
with open("customers.csv", mode="w") as file:
  writer = csv.writer(file)
  writer.writerow(header)
  for id in range(1,101):
    row=[]
    row.append(id)
    fname=fake.first_name()
    row.append(fname)
    lname=fake.last_name()
    row.append(lname)
    city = choice(cities)
    row.append(city)
    writer.writerow(row)