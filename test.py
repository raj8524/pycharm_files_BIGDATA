
# import re
# strings=("hippopotamus","po")
# def find_index(*args):
#     # indexes=True
#     # result=1
#     # x=0
#     # while x!=-1:
#     #     result=str1.find(str2,x)
#     #     print(result)
#     #     x=result
#             # indexes=True
#
# find_index(*strings)
findProduct=[10,3,5,87,34]
i=1
# for k in findProduct:
#     if k<10:
#         i=i*k
# print(i)
final_lst=[]
# for i in range(1,6):
#  final_lst.append([x for x in range(1,i+1)])
# print(final_lst)
def generateList(num):
 return [list(range(1,i)) for i in range(1,num+2) if i > 1]
print(generateList(5))
