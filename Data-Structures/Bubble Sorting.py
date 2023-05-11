l1=[4,1,5,2,9,8,5,0]
for i in range(len(l1)-1):
    for j in range(len(l1)-1):
        if l1[j]>l1[j+1]:
            print("list printed before exchange",l1)
            l1[j],l1[j+1]=l1[j+1],l1[j]
            print("list printed after exchange", l1)
        else:
            print("list without comparision",l1)

print(l1)