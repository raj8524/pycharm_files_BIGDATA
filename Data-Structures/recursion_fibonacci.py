def fibonacci(n):
    if n==0:
        return 0
    elif n==1:
        return 1
    else:
        return fibonacci(n-1)+fibonacci(n-2)

n=8
for k in range(1,n+1):
    print(fibonacci(k),end=" ")
print("\n")
l1=[fibonacci(k) for k in range(1,n+1)]
print(l1)
