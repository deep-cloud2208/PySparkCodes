# x = 10
#
# def a():
#     global x
#     x = 20
#     print("2 - ",x)
#
# print("1 - ",x)
# a()
# print("3 - ",x)

from pyspark.sql.functions import to_date, to_timestamp, col

x = 10
y = 10

print(id(x))
print(id(y))

import sys
print('dict: ',sys.getsizeof(dict()))
print('tuple: ',sys.getsizeof(tuple()))
print('list ',sys.getsizeof(list()))

import gc
print(gc.get_objects(0))