
class Database():
    import pickle
    import os

    def __init__(self, name=None):
        if name is None:
            print("Name is not given")
            return False

        self.fname = "storage/{}.db" . format(name)
        self.loaded = False
        self.columns = set()
        self.rows = []

        if not self.os.path.exists(self.fname):
            self.store()

    def load(self):
        fsize = self.os.path.getsize(self.fname)
        self.db = {
            "columns": set(),
            "rows": []
        }
        if fsize > 0:
            file = open(self.fname, 'rb')
            self.db = self.pickle.load(file)
            file.close()

        self.columns = self.db.get("columns", set())
        self.rows = self.db.get("rows", [])
        self.loaded = True

        return True

    def size(self):
        if not self.loaded:
            print("Database is not loaded")
            return 0

        return len(self.rows)


    def store(self):

        tmp = {
            "rows": self.rows,
            "columns": self.columns
        }

        if not self.os.path.exists(self.fname):
            """ File doesn't exist, make it """
            file = open(self.fname, 'wb').close()

        file = open(self.fname, 'wb')
        self.db = tmp
        self.pickle.dump(self.db, file)
        file.close()
        return True

    def create(self, columns):
        if not self.loaded:
            print("Database is not loaded")
            return False

        self.columns = columns
        return True

    def insert(self, row):
        """
        Row looks like dictionary.
        :param row:
        :return:
        """
        if not self.loaded:
            print("Database is not loaded")
            return False

        self.rows.append(row)
        return True

    def select(self, key, what, value):
        """
        Checking if key meets criteria of value
        :param key:
        :param what: can be ">", "=", "<", ">=", "<="
        :param value:
        :return:
        """
        if not self.loaded:
            print("Database is not loaded")
            return False

        if key not in self.columns:
            print("Key [{}] does not exist in your DB! " . format(key))

        tmp_results = []
        for row in self.rows:
            details = row.get(key, None)
            if details is None:
                pass
            if what == ">" and details > value:
                tmp_results.append(row)
            elif what == "<" and details < value:
                tmp_results.append(row)
            elif what == "=" and details == value:
                tmp_results.append(row)
            elif what == ">=" and details >= value:
                tmp_results.append(row)
            elif what == "<=" and details <= value:
                tmp_results.append(row)

        return tmp_results

    def select_all(self):
        return self.rows

    def order_by(self, results, key_, direction="ASC"):
        """
        Sort results by key either ASC or DESC
        :param key_:
        :param direction:  ASC or DESC
        :return:
        """

        return sorted(results, key=lambda x: x.get(key_), reverse=direction=="DESC")

    def select_random(self, results=None):
        from random import randint
        if results is None:
            r = randint(0, len(self.rows)-1)
            return self.rows[r]
        r = randint(0, len(results)-1)
        return results[r]

    def update(self, new_value, key, what, value):
        for e, row in enumerate(self.rows):
            details = row.get(key, None)
            if details is None:
                pass
            if what == ">" and details > value:
                self.rows[e][key] = new_value
            elif what == "<" and details < value:
                self.rows[e][key] = new_value
            elif what == "=" and details == value:
                self.rows[e][key] = new_value
            elif what == ">=" and details >= value:
                self.rows[e][key] = new_value
            elif what == "<=" and details <= value:
                self.rows[e][key] = new_value



""" TESTING """
if __name__ == "__main__":
    mydb = Database("test")

    columns = ["first_name", "last_name", "age"]

    data = [
        {"first_name": "John1", "last_name": "Doe1", "age": 25},
        {"first_name": "John2", "last_name": "Doe2", "age": 27},
        {"first_name": "John3", "last_name": "Doe3", "age": 20}
    ]

    mydb.load()
    if mydb.size() == 0:  # empty DB, fill it
        mydb.create(columns)
        for d in data:
            mydb.insert(d)
            pass

    res = mydb.select("age", ">", 20)

    mydb.store()  # store DB on disk (for permanent storage)

    print("Result: ", res)
    sorted_result = mydb.order_by(res, "age", "DESC")
    print("Sorted result: ", sorted_result)
    random_one = mydb.select_random()
    print("Random one: ", random_one)
    random_one = mydb.select_random(results=res)
    print("Random one from res: ", random_one)

    mydb.update("Johnny", "first_name", "=", "John1") # change John1 to Johnny
    res = mydb.select("age", ">", 20)
    print("Results after update: ", res)