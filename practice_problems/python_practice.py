sales = [250.5, 130.0, 980.75, 450.25, 75.0, 620.50, 390.0]

# Task 1 : sales above 400

for num in sales:
    if num > 400:
       print(num)
    else:
       pass

# Task 2 : total sales 
print("total sales : ", sum(sales))

# Task 3 : max 
print("max is ", max(sales))

# Task 4 : count below 300
counts = []
for i  in sales:
    if i < 300:
       counts.append(i)
    else:
        pass
print("count below 300 is ", len(counts))

# Task 5 : above 400
above_400 = []
for i in sales:
    if i > 400:
       above_400.append(i)
    else:
       pass
print(above_400)

## dictionary
order = {
    "order_id": 1001,
    "customer": "John Doe",
    "region": "West",
    "sales": 731.94,
    "category": "Technology"
}

print(order.keys())

print(order.update({"discount": 0.2}))

print(order["customer"])

print(order.values())

regions = "region"
if regions in order:
   print(regions)
else:
   print("does not exist")





















