#-*-coding:utf-8-*

class Data:
    def __init__(self, n = '', v = 0.0):
        self.name = n
        self.value = v

cur = Data('cur', 0)
ye2 = Data('ye2', 2)
ye3 = Data('ye3', 3)
ye5 = Data('ye5', 5)

m = {}
m[cur.name] = cur
m[ye2.name] = ye2
m[ye3.name] = ye3
m[ye5.name] = ye5
print ""
for (name, item) in m.items():
    print name,item.name,item.value

ye2.value = 20
ye3.value = 30
ye5.value = 50
print ""
for (name, item) in m.items():
    print name,item.name,item.value

cur.value = 1000
cur = Data('ttt', 999)
print ""
for (name, item) in m.items():
    print name,item.name,item.value