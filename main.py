from collections import defaultdict
import pickle

class DB:
    def __init__(self):
        self.db = defaultdict(list)

    def create(self, k, v):
        self.db[k] = [v]
        print "create success"

    def read_u(self, k):
        if not self.db[k]:
            raise KeyError("no key in database")
        else:
            return self.db[k]

    def read_s(self, k):
        try:
            print db.read_u(k)
        except KeyError as e:
            print e

    def update(self, k, v):
        self.db[k].append(v)
        print "update sucess"

    def delete(self, k):
        self.db[k] = list()
        print "delete success"

    def savetofile(self, filename):
        pickle.dump(self.db, open(filename, "wb"))

    def loadfromfile(self, filename):
        self.db = pickle.load(open(filename, "rb"))

DB_API_func_2 = {
    "read": lambda dbs, k: dbs.read_s(k) ,
    "delete": lambda dbs, k: dbs.delete(k),
    "savetofile": lambda dbs, fname: dbs.savetofile(fname),
    "loadfromfile": lambda dbs, fname: dbs.loadfromfile(fname)
}

DB_API_func_3 = {
    "create": lambda dbs, k, v: dbs.create(k, v),
    "update": lambda dbs, k, v: dbs.update(k, v),
}

db = DB()
'''
db.create("k1", 1)

print db.read_s("k1")
print db.read_s("k2")

db.create("k1", 2)

print db.read_s("k1")

db.update("k2", 3)
db.update("k2", 4)

print db.read_s("k2")

db.delete("k1")

print db.read_s("k1")

db.savetofile("mydb")

db.update("k1", 1)

db.loadfromfile("mydb")

db.read_s("k1")
'''
while True:
    command = raw_input().split()
    if command[0] == "exit":
        exit()
    if len(command) == 2:
        DB_API_func_2[command[0]](db, command[1])
    elif len(command) == 3:
        DB_API_func_3[command[0]](db, command[1], command[2])