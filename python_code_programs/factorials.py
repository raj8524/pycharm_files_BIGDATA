from math import factorial as fact,prod
from functools import reduce
"""
# m1
# =====
num=5
def fact(num):
    mul1 = 1
    for k in range(num,0,-1):
        for j in range(1,k+1):
            mul1=mul1*j
    return mul1
print(fact(num))


# m2
# ===========
result =[fact(i) for i in range(1,num)]
print(result)

# m3

def fact_of_fact(num):
    fac = reduce(lambda x,y:x*y,[fact(i) for i in range(1,num+1)])
    return fac

# print(fact_of_fact(num))

# m4
def reducing_factorial(num):
    fact = lambda x: x*fact(x-1) if x > 1 else 1
    res = reduce(lambda x, y: x*y, [fact(i+1) for i in range(num)])
    return res

# print(reducing_factorial(5))
"""
# m5
l2=[]
def factorial1(number):
    fact1 = 1
    for i in range(1, number+1):
        # fact1 *= fact(i)
        l2.append(fact(i))
    print(l2)
    return reduce(lambda x,y:x*y,l2)
    # return fact1

print(factorial1(5))