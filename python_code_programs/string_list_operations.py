"""
Ques::
uncensor("Wh*r* d*d my v*w*ls g*?", "eeioeo") ➞ "Where did my vowels go?"

uncensor("abcd", "") ➞ "abcd"

uncensor("*PP*RC*S*", "UEAE") ➞ "UPPERCASE"
un1=("*PP*RC*S*", "UEAE")
def find_str(a,b):
    a1=list(a)
    for k in range(len(b)):
        a1[a1.index("*")]=b[k]
    return "".join(a1)
print(find_str(un1[0],un1[1]))


def uncensor(st,vow_lst):
    for i in range(st.count('*')):
        st = st.replace('*',vow_lst[i],1)
    return st

print(uncensor("Wh*r* d*d my v*w*ls g*?", "eeioeo"))


s='wh*r* d*d my v*w*ls g*?'
v='eeioeo'
def uncensor(s,v):
    res = ""
    flag = 0
    for i in range(len(s)):
        if(s[i]=="*"):
            res = res+v[flag]
            flag = flag+1
        else:
            res = res+s[i]
    return res

print(uncensor(s,v))



m=list(s)
v=list(v)
j=0
for i in range(len(m)):
    if m[i]=='*':
        m.remove(m[i])
        m.insert(i,v[j])
        j=j+1
print(''.join(m))


def uncensor1(s, v):
    return s.replace("*", "{}").format(*v)

print(uncensor1(s,v))
"""