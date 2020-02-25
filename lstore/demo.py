from lstore.db import Database
from lstore.query import Query

db = Database()
num_columns = 5
grades_table = db.create_table('Grades', num_columns, 0)
query = Query(grades_table)

for i in range(512):
    query.insert(2000+i, 4, 3, 2, 2**64-1)

for j in range(512):
    print(j)
    for i in range(num_columns + 4):
        a = j*8
        b = a+8
        print(
            grades_table.partitions[0].base_page[i].data[a:b],
            int.from_bytes(
                grades_table.partitions[0].base_page[i].data[a:b],
                byteorder='big',
                signed=False),
            sep='\t'
        )
    print()

result = query.select(2001, [1, 1, 1, 1, 1])[0]
print(result.rid, result.key, result.columns)

query.update(2001, *[None, 1, None, None, 1])
result = query.select(2001, [1, 1, 1, 1, 1])[0]
print(result.rid, result.key, result.columns)

query.update(2001, *[i for i in range(10, 15)])
result = query.select(2001, [1, 1, 1, 1, 1])[0]
print(result.rid, result.key, result.columns)

for j in range(2):
    print(j)
    for i in range(num_columns + 4):
        a = j*8
        b = a+8
        print(
            grades_table.partitions[0].tail_pages[0][i].data[a:b],
            int.from_bytes(
                grades_table.partitions[0].tail_pages[0][i].data[a:b],
                byteorder='big',
                signed=False),
            sep='\t'
        )
    print()
# print(result.rid, result.key, result.columns)
