#1
a = 10
b = 20.0
c = b ** a
d = str(c)
x = (c==d)
print("1:",x)
print(type(x))

#2
i = str('100')
j = int(i) // 10
x = j / 2
print("2:",x)
print(type(x))

#3
m = 11 % 100
n = 2.0 ** 10
x = str(int(m+n)) * 2
print("3:",x)
print(type(x))

#4
age = 29
sex = 'female'
height = 165
weight = 48
x = (sex == 'female') and (
    (age < 25) or
    ((height > 150) and (height < 170) and (weight >= 40) and (weight <= 50))
)
print("4:",x)
print(type(x))