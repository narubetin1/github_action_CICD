#2.1
Cost_baht = int(input("cost_baht: "))
Customer_baht  = int(input("Customer_baht: "))

total_output = 0
if Customer_baht > Cost_baht :
    total_output = Customer_baht - Cost_baht
    print("Pay to Customer:",total_output,"baht")
else :
    print("Can't turn pay to you!")

#2.2
cost_food = float(input("Cost_food: "))
discount_percent = float(input("Discount: "))

discounted_price = cost_food * (1 - discount_percent / int(100))
vat = discounted_price * 0.07
service_charge = discounted_price * 0.1

total_to_pay = discounted_price + vat + service_charge

print("Total price to pay:" , total_to_pay, "baht")


#2.3
width_input = float(input("wide: "))
Length_input = float(input("Length: "))

area = width_input * Length_input
print("Area:",area ,"unit")