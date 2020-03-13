

# PYTHON IS NOT A TYPE SAFE PROGRAMMING LANGUAGE

# -------------------------------------------------------------------------------
# Variables and Literals
# -------------------------------------------------------------------------------

# a,b,c = 10,20,30
# d,e = 'Intellipaat',100.50
# multiline_var = '''
# hi everyone, welcome to python course
# this is a multi line variable
# '''
# bool_var = False
# none_var = None

# print('type of a: ', type(a))
# print('type of b: ',type(b))
# print('type of c: ',type(c))
# print('type of bool_var: ',type(bool_var))
# print('type of none_var: ',type(none_var))
# print('value of a: ',a)
# print('value of b: ',b)
# print('value of c: ',c)
# print('value of d: ',d)
# print('value of e: ',e)
# print('value of multi: ',multiline_var)
# -------------------------------------------------------------------------------

# -------------------------------------------------------------------------------
# Arithmetic Operators
# -------------------------------------------------------------------------------
# a = 10
# b = 20
# c = a + b
# print('c after addition: ',c)
# print('a+b: ',a+b)
# print('a*b: ',a*b)
# print('a-b: ',a-b)
# print('a/b: ',a/b)
# print('a%b: ',a%b)

# -------------------------------------------------------------------------------
# Assignment Operators
# -------------------------------------------------------------------------------
# a = 10
# a += 10 #(a = a+10)
# a -= 5 #(a = a-5)
#
# print(a)

# -------------------------------------------------------------------------------
# Logical Operators
# -------------------------------------------------------------------------------

# a = True
# b = False
#
# if a & b:
#     print('at least one of them is true')


# -------------------------------------------------------------------------------
# Tuples () - NON MODIFIBALE (Immutable) ARRAY/LIST
# -------------------------------------------------------------------------------

# var_tuple_technology = ('aws', 'spark', 100, 200.5, 'gcp', 'hadoop','aws','aws')
# var_tuple_technology = tuple() # empty tuple

# print(var_tuple_technology[3])
# print(var_tuple_technology.count('aws'))
# print(len(var_tuple_technology))

# count=0
# for i in var_tuple_technology:
#     print(i)
#     count=count+1
#
# print(count)

# -------------------------------------------------------------------------------
# Lists [] - MODIFIBALE (Mutable) ARRAY/LIST
# -------------------------------------------------------------------------------

# var_list_technology = ['aws','spark',100,'hadoop','yarn']

# var_list_technology = list() # Create empty list

# var_list_technology[2] = 'spark' # Not allowed

# var_list_technology.append('aws')
# var_list_technology.append('gcp')
# var_list_technology.append('azure')
# var_list_technology.append('spark')
# var_list_technology.append('hadoop')
#
# var_list_technology.insert(3,'Big Data')
#
#
# print(len(var_list_technology))
# print(var_list_technology)

# var_list_technology.sort(reverse=True)

# Pop pulls variable out in LIFO mode

# print(var_list_technology)
#
# print(var_list_technology.pop())
# print(var_list_technology.pop())
# print(var_list_technology.pop())
# print(var_list_technology.pop())
#
# print(var_list_technology)



# -------------------------------------------------------------------------------
# Dictionaries - KEY VALUE Pair
# -------------------------------------------------------------------------------

# var_dict_employee = dict()
#
# var_dict_employee = \
#     {
#         "Name" : "xyz",
#         "Institute" : "Intellipaat",
#         "Address" : {
#             "Line1" : "India",
#             "Line2" : "Bangalore",
#             "ZipCode" : 560000,
#             "phone" : [12345678, 987654321, 99887766]
#          },
#         "Technologies" : ['aws','spark','hadoop'],
#         "Salary" : 100000
#     }


# print(type(address))
# print(var_dict_employee['Technologies'][2])
# print(var_dict_employee['Address']['phone'][1])
# print(var_dict_employee['Address']['ZipCode'])
# print(address['ZipCode'])

# var_dict_employee['Department'] = 'Big Data'
# # var_dict_employee = dict()
#
# var_dict_employee.update({"Name" : "India", "Weather" : "hot", "PrimeMinister" : "Mr. Narendra Modi"})
#
# print(var_dict_employee)

# var1 = var_dict_employee.pop('Name')
# print(var_dict_employee)
# print('value of var1: ',var1)

# print(var_dict_employee.keys())
# print(var_dict_employee.values())


# -------------------------------------------------------------------------------
# Sets - Unordered list/array with NON-DUPLICATE values
# -------------------------------------------------------------------------------

# var_set_technology = set()
# var_set_technology1 = {'aws', 'spark', 'hadoop', 'gcp'}
# var_set_technology2 = {'aws', 'spark', 'oracle', 'db2', 'mysql', 'mongodb'}
# var_set_technology3 = {'aws', 'spark', 'oracle', 'db2', 'mysql', 'mongodb', 'redis'}

# var_set_technology = var_set_technology3.difference(var_set_technology2)

# var_set_technology = var_set_technology1.intersection(var_set_technology2)

# var_set_technology.add('gcp')
# var_set_technology.add('spark')
# var_set_technology.remove('aws')
# var_set_technology2.pop()

# print(var_set_technology)


# -------------------------------------------------------------------------------
# Flow Control - IF statement
# -------------------------------------------------------------------------------

# var1 = 10
# var2 = 20
# var3 = 30
#
# if ((var2 > var1) and (var3 > var1) and (var3 > var2)):
#     print('var3 is biggest')
# elif (var2 > var1):
#     print('var2 is bigger')
# else:
#     print('var1 is smallest')

# if (var2 > var1):
#         print('var3 is bigger')
# elif (var3 > var2):
#     print('var3 is biggest')
# else:
#     print('var2 is bigger')


# -------------------------------------------------------------------------------
# Flow Control - FOR Statement (refer List, Set and Tuples)
# -------------------------------------------------------------------------------

# my_list = list()
# my_list = [1,2,3,4,5]
# a=0
#
# for i in my_list:
#     # my_list.append(i)
#     # print(my_list)
#     print(i)
#     print('hello all')
#     a = a+5
#     print(a)
#
# print(a)

# print(my_list)

# -------------------------------------------------------------------------------
# Convert from one data type to another
# -------------------------------------------------------------------------------
# a = '123'
# print(type(a))
# a = int(a)
# print(type(a))
# b = 345.00
# print(type(b))
# b = str(b)
# print(type(b))

# From Tuple to Set
# my_tuple = (1,2,3,4)
# print(type(my_tuple))
# my_set = set(my_tuple)
# print(type(my_set))
# print(my_set)

# From Set to Tuple
# my_new_tuple = tuple(my_set)
# print(type(my_new_tuple))
# print(my_new_tuple)

# From Tuple to List
# my_list = list(my_tuple)
# print(type(my_list))
# print(my_list)

# From List to String
# my_list = ['my', 'name', 'is', 'deep']
# print(type(my_list))
# my_str = ' '.join(my_list)
# print(type(my_str))
# print(my_str)

# From String to List

# my_new_list = my_str.split(' ')
# print(my_new_list)

# -------------------------------------------------------------------------------
# File Handling
# -------------------------------------------------------------------------------

# file = open('sample-data.csv','r')

# print(file.readline())
# print(file.read())

# with open('sample-data.csv', 'r') as file:
#     for i in file.read():
#         print(i)
#
# my_list = ['a','b']
#
# with open('sample-data-ouput.csv', 'w') as file:
#     for i in my_list:
#         i = str(i)
#         file.write(i)
#
# my_list = ['Intellipaat', 'PySpark', 'Training']
#
# with open('sample-data-ouput.csv', 'a') as file:
#     for i in my_list:
#         i = str(i)
#         file.write(i)



















