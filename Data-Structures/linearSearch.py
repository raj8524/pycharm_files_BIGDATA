A=[84,91,76,8,0]
def linearSearch(A,key):
    position=0
    flag=False
    while position <len(A) and not flag:
        if A[position]==key:
            flag=True
        else:
            position +=1
    return flag
key=9
found=linearSearch(A,key)
print("element present {key} : ",key,found)