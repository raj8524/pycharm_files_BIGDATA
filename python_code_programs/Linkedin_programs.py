"""
# Given two matrix the task is that we will have to create a program to multiply two matrices in python.
x = [[1, 7, 3],
[3, 5, 6],
[6, 8, 9]]

y = [[1, 1, 1, 2],
[6, 7, 3, 0],
[4, 5, 9, 1]]

Output : [[55, 65, 49, 5],
[57, 68, 72, 12],
[90, 107, 111, 21]]

x_rows = len(x)
x_colns = len(x[0])
y_colns = len(y[0])

ans = [[2]*y_colns for k in range(x_rows)]
print(ans)
for i in range(x_rows):
    for j in range(y_colns):
        ans[i][j] = sum(x[i][k]*y[k][j] for k in range(x_colns))
        print(ans[i][j])

for op in ans:
    print(op)


Write a function that converts hours into seconds.
Examples
how_many_seconds(2) ➞ 7200

how_many_seconds(10) ➞ 36000

how_many_seconds(24) ➞ 86400
z="2:30:10"
l1=[]
l1.append(z.split(":"))
def count_sec(*l1):
    total_second = 0
    total_second=int(l1[0][0])*60*60+int(l1[0][1])*60+int(l1[0][2])
    return total_second
print(count_sec(*l1))


alphabet = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]
str1="Andrey".lower()
ind1=str1.index(str1[0])
heg1=0
str2=""
for i in str1:
    if i in alphabet:
        heg1=alphabet.index(i)
        if heg1>ind1:
            ind1=heg1
x=ind1+1
print(str(x)+alphabet[ind1])


def alphabet_index(alphabet,st):

    max_ch = st[0]
    for i in range(1,len(st)):
        if st[i]>max_ch:
            max_ch=st[i]
    print(ord(max_ch))
    return str(ord(max_ch)-96)+max_ch

print(alphabet_index(alphabet, "Flavio"))
print(alphabet_index(alphabet, "Andrey"))
print(alphabet_index(alphabet, "Oscar"))
print(alphabet_index(alphabet, "abababab"))


Ques::
========
write program to sort even position number in asc and odd position in desc:

l1=[13,2,4,15,12,10,5]
def func(l1):
    even=sorted(l1[0::2],reverse=True)
    odd=sorted(l1[1::2])
    required_list=[0]*len(l1)
    required_list[0::2]=even
    required_list[1::2] = odd
    return required_list

print(func(l1))

"""






