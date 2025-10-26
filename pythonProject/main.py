num_kg = int(input("num_kg: "))
cost_kg = int(input("cost_kg: "))

total_cost = 0
if num_kg > 0 and cost_kg > 0 :
    total_cost = num_kg * cost_kg
print(total_cost)
