"""
ascending order list
=====================
for i in range(len(l1)):
    min_value=min(l1[i:])
    print(f"value of min_value at {i} iteration ",min_value)
    min_index=l1.index(min_value,i)
    print(f"value of min_index at {i} iteration",min_index)
    print("before change",l1[i],l1[min_index])
    print("list before change",l1)
    l1[i],l1[min_index]=l1[min_index],l1[i]
    print("after change", l1[i], l1[min_index])
    print("list after change", l1)

print(l1)
"""

for i in range(len(l1)):
    max_value=max(l1[i:])
    print(f"value of min_value at {i} iteration ",max_value)
    max_index=l1.index(max_value,i)
    print(f"value of min_index at {i} iteration",max_index)
    print("before change",l1[i],l1[max_index])
    print("list before change",l1)
    l1[i],l1[max_index]=l1[max_index],l1[i]
    print("after change", l1[i], l1[max_index])
    print("list after change", l1)

print(l1)