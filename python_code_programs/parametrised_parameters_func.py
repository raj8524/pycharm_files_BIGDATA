"""
OREDER OF PARAMETERS passed to function

positional arg,keyword args,default args

positional arg,*args,keyword args,**kwargs
"""
def abc(k,dunzo,zepto="jldi lao"):
    print(k)
    print(dunzo)
    print(zepto)
abc(5,dunzo="sbji")

y={
    "name":"raj",
    "age":30,
    "address":{"city":"ssm"}
}
z=(4,4,5)
a=10
def xyz(a,*z,b,**y,):
    sum1 = 0
    print(b)
    print("value of a {}".format(a))
    for i in z:
        print(i)
        sum1=sum1+int(i)
        print(sum1)

    for k in y.values():
        print(k)

xyz(a,*z,b=5,**y)
