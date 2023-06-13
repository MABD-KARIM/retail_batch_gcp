import csv

header = ["StockDate","ProdId","StockQty"]
with open("initial_stock.csv", mode="w") as file:
  writer = csv.writer(file)
  writer.writerow(header)
  for i in range(1,41):
    row=[]
    row.append("2019-12-31")
    row.append(i)
    row.append(100)
    writer.writerow(row)