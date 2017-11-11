from Database import Database as DB

mydb = DB("example")

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