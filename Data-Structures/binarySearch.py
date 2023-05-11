def binary_search(arr1,n):
    low=0
    high=len(arr1)-1
    mid=0
    while(low<=high):
        mid=(low+high)//2
        if arr1[mid]<n:
            low=mid+1
        elif arr1[mid]>n:
            high=mid-1
        else:
            return mid

    return -1

list1 = [12, 24, 32, 39, 45, 50, 54]
n = 0
result=binary_search(list1,n)

if result !=-1:
    print("element present at index",str(result))
else:
    print("element no present")