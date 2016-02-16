from collections import defaultdict
import pickle
import uuid

key = 0

class DB:
    def __init__(self):
        self.db = defaultdict(list)

    def enable_repl(self):
        self.db["complete"] = key
        self.savetofile("db1_repl")
        self.savetofile("db2_repl")

    def read_u(self, k):
        if not self.db[k]:
            raise KeyError("no key in database")
        else:
            return self.db[k]

    def read_s(self, k):
        try:
            print self.read_u(k)
        except KeyError as e:
            print e

    def read_s_repl(self, k):
        self.no_change_repl_2(k, lambda dbs, k: dbs.read_s(k))

    def no_change_repl_2(self, k, lbd):
        db1 = DB()
        db2 = DB()
        db1_flag = False
        db2_flag = False
        try:
            db1.loadfromfile("db1_repl")
        except IOError as e:
            print "db recovered"
        else:
            db1_flag = True
        try:
            db2.loadfromfile("db2_repl")
        except IOError as e:
            print "db recovered"
        else:
            db2_flag = True
        done1 = False
        done2 = False
        if db1_flag:
            if db1.db["complete"] == key:
                lbd(db1, k)
                done1 = True
        if (not done1) and db2_flag:
            if db2.db["complete"] == key:
                lbd(db2, k)
                done2 = True
        if (not done1) and (not done2):
            raise MemoryError("db corrupted")

    def create(self, k, v):
        self.db[k] = [v]
        print "create success"

    def create_repl(self, k, v):
        self.change_repl_3(k, v, lambda dbs, k, v: dbs.create(k, v))

    def update(self, k, v):
        self.db[k].append(v)
        print "update sucess"

    def update_repl(self, k, v):
        self.change_repl_3(k, v, lambda dbs, k, v: dbs.update(k, v))

    def change_repl_3(self, k, v, lbd):
        global key
        db1 = DB()
        db2 = DB()
        db1_flag = False
        db2_flag = False
        try:
            db1.loadfromfile("db1_repl")
        except IOError as e:
            print "db recovered"
        else:
            db1_flag = True
        try:
            db2.loadfromfile("db2_repl")
        except IOError as e:
            print "db recovered"
        else:
            db2_flag = True
        done1 = False
        done2 = False
        if db1_flag:
            if db1.db["complete"] == key:
                lbd(db1, k, v)
                done1 = True
        if (not done1) and db2_flag:
            if db2.db["complete"] == key:
                lbd(db1, k, v)
                done2 = True
        if (not done1) and (not done2):
            raise MemoryError("db corrupted")
        if done1:
            newkey = uuid.uuid1()
            db1.db["complete"] = newkey
            key = newkey
            db1.savetofile("db1_repl")
            db1.savetofile("db2_repl")
        if done2:
            newkey = uuid.uuid1()
            db2.db["complete"] = newkey
            key = newkey
            db2.savetofile("db1_repl")
            db2.savetofile("db2_repl")

    def delete(self, k):
        self.db[k] = list()
        print "delete success"

    def delete_repl(self, k):
        self.change_repl_2(k, lambda dbs, k: dbs.delete(k))

    def change_repl_2(self, k, lbd):
        global key
        db1 = DB()
        db2 = DB()
        db1_flag = False
        db2_flag = False
        try:
            db1.loadfromfile("db1_repl")
        except IOError as e:
            print "db recovered"
        else:
            db1_flag = True
        try:
            db2.loadfromfile("db2_repl")
        except IOError as e:
            print "db recovered"
        else:
            db2_flag = True
        done1 = False
        done2 = False
        if db1_flag:
            if db1.db["complete"] == key:
                lbd(db1, k)
                done1 = True
        if (not done1) and db2_flag:
            if db2.db["complete"] == key:
                lbd(db2, k)
                done2 = True
        if (not done1) and (not done2):
            raise MemoryError("db corrupted")
        if done1:
            newkey = uuid.uuid1()
            db1.db["complete"] = newkey
            key = newkey
            db1.savetofile("db1_repl")
            db1.savetofile("db2_repl")
        if done2:
            newkey = uuid.uuid1()
            db2.db["complete"] = newkey
            key = newkey
            db2.savetofile("db1_repl")
            db2.savetofile("db2_repl")

    def savetofile(self, filename):
        pickle.dump(self.db, open(filename, "wb"))

    def loadfromfile(self, filename):
        self.db = pickle.load(open(filename, "rb"))

    def savetofile_repl(self, filename):
        db1 = DB()
        db2 = DB()
        db1_flag = False
        db2_flag = False
        try:
            db1.loadfromfile("db1_repl")
        except IOError as e:
            print "db recovered"
        else:
            db1_flag = True
        try:
            db2.loadfromfile("db2_repl")
        except IOError as e:
            print "db recovered"
        else:
            db2_flag = True
        done1 = False
        done2 = False
        if db1_flag:
            if db1.db["complete"] == key:
                pickle.dump(self.db, open(filename, "wb"))
                db1.savetofile(filename)
                done1 = True
        if (not done1) and db2_flag:
            if db2.db["complete"] == key:
                pickle.dump(self.db, open(filename, "wb"))
                db2.savetofile(filename)
                done2 = True
        if (not done1) and (not done2):
            raise MemoryError("db corrupted")
        print "save success"

    def loadfromfile_repl(self, filename):
        db1 = DB()
        db1_flag = False
        try:
            db1.loadfromfile(filename)
        except IOError as e:
            raise e
        else:
            db1_flag = True
        done1 = False
        if db1_flag:
            if db1.db["complete"] == key:
                self.db = pickle.load(open(filename, "rb"))
                self.savetofile("db1_repl")
                self.savetofile("db2_repl")
                done1 = True
        if not done1:
            raise MemoryError("db corrupted")
        print "load success"



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

DB_API_func_2_repl = {
    "read": lambda dbs, k: dbs.read_s_repl(k) ,
    "delete": lambda dbs, k: dbs.delete_repl(k),
    "savetofile": lambda dbs, fname: dbs.savetofile_repl(fname),
    "loadfromfile": lambda dbs, fname: dbs.loadfromfile_repl(fname)
}

DB_API_func_3_repl = {
    "create": lambda dbs, k, v: dbs.create_repl(k, v),
    "update": lambda dbs, k, v: dbs.update_repl(k, v),
}

db = DB()
key = uuid.uuid1()
db.enable_repl()
print "new db created"
print "API:"
print "create, update, delete, read, savetofile, readfromfile"
while True:
    print ">>",
    command = raw_input().split()
    if command[0] == "exit":
        exit()
    if len(command) == 2:
        DB_API_func_2_repl[command[0]](db, command[1])
    elif len(command) == 3:
        DB_API_func_3_repl[command[0]](db, command[1], command[2])