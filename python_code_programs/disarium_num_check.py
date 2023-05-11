"""
#m1
num=135
#1**1+3**2+5**3
a=dict(zip(range(1, len(str(num))+1), str(num)))
print(a)
k=a.keys()
v=a.values()
print (True if sum(list(map(lambda x, y:int(y)**x, k, v))) ==num else False)


#m2
num=544
sum1 = 0
def disarium_check(num):
 total_sum=0
 l1=list(str(num))
 for k,v in enumerate(l1):
 total_sum=total_sum+(pow(int(v), k + 1))
 sum1=total_sum
 return int(sum1)
if disarium_check(num)==num:
 print("{} disarium number".format(num))
else:
 print("{} not disarium number".format(num))

#m3
 def is_disarium(n):
  s = str(n)
  x = sum([int(ch) ** (index + 1) for index, ch in enumerate(s)])
  print(x)
  return sum([int(ch) ** (index + 1) for index, ch in enumerate(s)]) == n

print(is_disarium(136))

#m4
def is_disarium(n):
  sum,j=0,0
  n_str=str(n)
  for i in range(1,len(n_str)+1):
  new_n=int(n_str[j:i])
  sum+=new_n**i
  j=j+1
  if n==sum:
  print("True")
  else:
  print("False")
is_disarium(75)
is_disarium(135)

"""