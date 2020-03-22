#-------------------------------------------------------------------------------
# Python Functions/Methods
# Two types - i) user defined functions - which we define. we write the code
#                                         in the function.
#            ii) built-in functions - open(), list(), set(), tuple(), dict(),
#                                     type(), str(), int(), min(), max(), avg()
#
# How to call a Function - <function-name>(arg) - func(a)
#
#-------------------------------------------------------------------------------
# Basic Function
#-------------------------------------------------------------------------------
# def myFunc():
#     print("Hello there. Welcome to Day 3. I am inside of function")
#     print("this the second line inside of the function")
#
# myFunc()

# import sys

# def myFunc(a,b): # a=20, b=30
#     c = a+b
#     # return c
#     return (a*b)
#
# x = myFunc(10,100)
# print(x)

# TechStack = ["Spark", "Hive", "Sqoop"]

# topic = sys.argv[1] # Receive command line argument
# topic=str(topic).lower()
#
# print("printing the command line argument that was passed: ", topic)
#
# if topic == "spark":
#     x = myFunc(10,20)
#     print(x)
# else:
#     print("Please learn spark")


# print("hi there")
# var1 = myFunc(20,30) # var1 would have the value after "return" statement, which is value of c
# print(var1)

#-------------------------------------------------------------------------------
# Pass List/Tuple/Set as arguments
#-------------------------------------------------------------------------------
# def ArrayFunc(array):
#     print("a has been captured in 'array' variable: ", array)
#     # funcVar = array[0]
#     funcVar = array.pop()
#     return funcVar

a = list()
# a.append('Spark')
# a.append('Python')
# a.append('PySpark')
# a.append('HDFS')
# a.append('MapReduce')

# a = [ 'Spark', 'Python', 'PySpark', 'HDFS', 'MapReduce']
#
# b=ArrayFunc(a)
# print(b)

# a = tuple()
# a = ('Spark', 'Python', 'PySpark')
# ArrayFunc(a)

# a = set()
# a = {'Spark', 'Python', 'PySpark', 'Spark', 'Python', 'PySpark'}
#
# var1 = a
# print("var1 (before passing to ArrayFunc): ",var1)
#
# print("a (outside of ArrayFunc): ", a)
# var1 = ArrayFunc(a)
# print("var1 (after coming from ArrayFunc): ",var1)


# def func_to_return_multiple_values(array):
    # var1 = array[0]
    # var2 = array[1]
    # var1_set = array.pop()
    # var2_set = array.pop()
    # return var1, var2
    # return var1_set, var2_set

# var = ("Deep","AWS", "GCP")
# var = [1,2,3,4]
# var = {1,2,3,4}
# var_out = func_to_return_multiple_values(var)
# print("Getting multiple values in 'return' statement: ", var_out)
# var_out1 = func_to_return_multiple_values(var)
# var_out2 = func_to_return_multiple_values(var)[1]
# print(var_out1)
# print(var_out2)

#-------------------------------------------------------------------------------
# Pass Dictionary as argument
#-------------------------------------------------------------------------------
# def DictFunc(myDict):
#     subject = myDict['subject']
#     additional = myDict['good_to_have']
#     return subject,additional
#
# var = dict()
# var['name'] = 'Training'
# var['subject'] = 'Spark'
# var['programming_lang'] = 'Python'
# var['tech_family'] = ['Hadoop', 'Big Data', 'Apache Spark', 'Hadoop', 'HDFS', 'MapReduce']
# var['good_to_have'] = \
#     {
#         'Cloud1': 'AWS',
#         'Cloud2': 'GCP',
#         'Programming1': 'Scala',
#         'Programming2': 'Java'
#     }
# var['bussiness_use_case'] = 'Data Warehouse and Analytics'
#
# print("var (before passing to DictFunc): ",var)
#
# var_out1=DictFunc(var)
# print(var_out1)

# var_out1=DictFunc(var)[0]
# var_out2=DictFunc(var)[1]
# print("subject: ", var_out1)
# print("optional: ", var_out2)
# print(var_out2['Programming1'])

#-------------------------------------------------------------------------------
# Pass multiple arguments to a function
#-------------------------------------------------------------------------------
# def MultiArgFunc(myDictArg, myArrayArg):
#     subject = myDictArg['Name']
#     prereq = myArrayArg[1]
#     return subject,prereq
#
# myDict = dict()
# myDict['Name'] = 'Apache Spark'
# myDict['Programming_lang'] = 'Python'
#
# myArray = list()
# myArray = ['Maths', 'Chemistry', 'Physics', 'Biology']
#
# MultiArgFunc(myDict,myArray)
# print(MultiArgFunc(myDict,myArray))

#-------------------------------------------------------------------------------
# Args and Kwargs - pass as many arguments as you want
#-------------------------------------------------------------------------------
# def ArgKwargFunc(*args, **kwargs):
# def ArgKwargFunc(arg1,*args, **kwargs):
#     print("arg1: ", arg1)
#     print("args: ", args)
#     print("kwargs: ", kwargs)
#
# mylist = ['a','b']
# myset = {'set1', 'set2'}
# mytuple = (1,2)
# mydict1 = {"FName" : "Soumyadeep", "LName" : "Dey", "Exp": 16}
# mydict2 = {"Tech": ['aws', 'gcp', 'big data']}

# mylist = "mylist"
# myset = "myset"
# mytuple = 100


# ArgKwargFunc(mylist,myset,mytuple,mydict1,mydict2,name="Deep",tech="Spark",exp=16,cloud=True)
# ArgKwargFunc(mylist,myset,mytuple)
# ArgKwargFunc(name="Intellipaat",subject="PySpark")
# ArgKwargFunc(mylist,myset,mytuple,Name="Intellipaat", Type="Training Institute")

#-------------------------------------------------------------------------------
# Call by Value & Call by Reference
#-------------------------------------------------------------------------------
# 1) Call by value is passing normal variables like Integer, Decimal, String
# 2) Call by Reference is passing List/Array. As the variable only contains
#    pointer to the Array, changes to the items inside function impacts it
#    outside
#-------------------------------------------------------------------------------
# a1 = [10,20,30]
# a = {10,20,30}
# a = (10,20,30)
# a2 = 10
#
# def func_call_by_value(b):
#     print("b (before assignment): ", b)
#     b = 20
#     print("b (after assignment): ", b)
#
# def func_call_by_reference(b):
#     print("b (before assignment): ", b)
#     b[1] = 100
#     b[2] = 1000
#     print("b (after assignment): ", b)
#

# func_call_by_value(a2)
# print("a2 (after returning from func - call by value): ", a2)
#
# func_call_by_reference(a1)
# print("a1 (after returning from func - call by reference): ", a1)

# -------------------------------------------------------------------------------
# Variable scope - Local and Global
# Note: Not used that much
# -------------------------------------------------------------------------------
# def ScopeFunc():
    # global x
    # global y
    # print(x)
    # x = "PySpark - LEGB"
    # y = "Y is Inside ScopeFunc"
    # print("<INSIDE ScopeFunc, after assignment> value of x: ", x)
    # print("<INSIDE ScopeFunc, after assignment> value of y: ", y)
    # print('  ')

# def ScopeFunc(y):
#     print("x: ",x)
#     print("y: ",y)
#     y = "Y in ScopeFunc - DeepSpark"
#     x = "X in ScopeFunc - DeepSpark"
#     print(y)
#     print(x)

# x = 'DeepSpark'

# print("'x' before passing to ScopeFunc: ", x)
# print("'y' before passing to ScopeFunc: ", y)
# print('  ')
# ScopeFunc()
# print("'x' after coming from ScopeFunc: ", x)
# ScopeFunc(x)
# print("'x' after coming from ScopeFunc (NOTICE THE VALUE, it did not change): ", x)
# print("'y' after coming from ScopeFunc (NOTICE THE VALUE, it was changed in ScopeFunc): ", y)
# print('  ')


#-------------------------------------------------------------------------------
# Lambda Functions (Also called Anonymous Functions or Lambda Expressions)
# Few traits:
# - Cannot be used to define complex logic. Cannot have multiple lines or else it will give error.
# - Mostly used in Data processing frameworks like Spark.
# - RETURN statement is not needed.
# - Following is the structure to define Lambda function
#   lambda x,y,z: <return expression>.
# - To call a Lambda function use the following syntax
#   i) Define the function as a variable > f = lambda x: x+1
#   ii) Call the variable with argument > f(3)
#-------------------------------------------------------------------------------
# def addition(var1,var2):
#     a = var1.split(' ')
#     b = var2
#     return a,b
#
# a1 = "my name is corona virus i will kill everyone"
# a2 = 100
# # c = addition(a1,a2)
# # print(c)
# b = lambda x,y: (x.split(' '),y)
# d = b(a1,a2)
# print(d)

