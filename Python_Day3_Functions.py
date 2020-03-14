#-------------------------------------------------------------------------------
# Python Functions/Methods
# Two types - i) user defined functions
#            ii) built-in functions
#-------------------------------------------------------------------------------
# Basic Function
#-------------------------------------------------------------------------------
# def myFunc():
#     print("Hello there. Welcome to Day 3. I am inside of function")
#
# myFunc()


# def myFunc(a,b):
#     return (a+b)
#     # return (a*b)
#
# var1 = myFunc(20,30)
# print(var1)

#-------------------------------------------------------------------------------
# Pass List/Tuple/Set as arguments
#-------------------------------------------------------------------------------
# def ArrayFunc(array):
#     funcVar = array.pop()
#     # funcVar = array[0]
#     return funcVar

# a = list()
# a.append('Spark')
# a.append('Python')
# a.append('PySpark')
# a.append('HDFS')
# a.append('MapReduce')

# a = [ 'Spark', 'Python', 'PySpark']

# a = tuple()
# a = ('Spark', 'Python', 'PySpark')

# a = set()
# a = {'Spark', 'Python', 'PySpark', 'Spark', 'Python', 'PySpark'}
#
# var1 = a
# print("var1 (before passing to ArrayFunc): ",var1)
#
# var1 = ArrayFunc(a)
# print("var1 (after coming from ArrayFunc): ",var1)
#

# def func_to_return_values(array):
    # var1 = array[0]
    # var2 = array[1]
    # var1_set = array.pop()
    # var2_set = array.pop()
    # return var1, var2
    # return var1_set, var2_set

# var = (1,2,3,4)
# var = [1,2,3,4]
# var = {1,2,3,4}
# var_out = func_to_return_values(var)
# print(var_out)
# var_out1 = func_to_return_values(var)[0]
# var_out1 = func_to_return_values(var)[1]

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
# var_out1=DictFunc(var)[0]
# var_out2=DictFunc(var)[1]
# print("subject: ", var_out1)
# print("optional: ", var_out2)

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
# # MultiArgFunc(myDict,myArray)
# print(MultiArgFunc(myDict,myArray))

#-------------------------------------------------------------------------------
# Call by Value & Call by Reference
#-------------------------------------------------------------------------------
# 1) Call by value is passing normal variables
# 2) Call by Reference is passing List/Array. As the variable only contains
#    pointer to the Array, changes to the items inside function does not impact
#    does not impact it outside
#-------------------------------------------------------------------------------
# a = [10,20,30]
# a = {10,20,30}
# a = (10,20,30)
# a = 10
# def chageit(b):
#     print("b: ", b)
    # b = 20
    # b[1] = 100
    # b[2] = 1000
    # print("b: ", b)

# print("a: ", a)
# chageit(a)
# print("a: ", a)

#-------------------------------------------------------------------------------
# Args and Kwargs - pass as many arguments as you want
#-------------------------------------------------------------------------------
# def ArgKwargFunc(*args, **kwargs):
# def ArgKwargFunc(arg1,*args, **kwargs):
#     print("arg1: ", arg1)
#     print("args: ", args)
#     print("kwargs", kwargs)
#
# mylist = ['a','b']
# myset = {'set1', 'set2'}
# mytuple = (1,2)
# mydict1 = {"FName" : "Soumyadeep", "LName" : "Dey", "Exp": 16}
# mydict2 = {"Tech": ['aws', 'gcp', 'big data']}

# ArgKwargFunc(mylist,myset,mytuple,mydict1,mydict2,name="Deep",tech="Spark",exp=16,cloud=True)
# ArgKwargFunc(mylist,myset,mytuple)
# ArgKwargFunc(mylist,myset,mytuple,Name="Intellipaat", Type="Training Institute")

# -------------------------------------------------------------------------------
# Variable scope - Local and Global
# Note: Not used that much
# -------------------------------------------------------------------------------
# def ScopeFunc():
#     # global x
#     # global y
#     # print(x)
#     x = "PySpark - LEGB"
#     # y = "Y is Inside ScopeFunc"
#     print("<INSIDE ScopeFunc, after assignment> value of x: ", x)
#     # print("<INSIDE ScopeFunc, after assignment> value of y: ", y)
#     print('  ')

# def ScopeFunc(y):
#     print("x: ",x)
#     print("y: ",y)
#     y = "Y in ScopeFunc - DeepSpark"
#     x = "X in ScopeFunc - DeepSpark"
#     print(y)
#     print(x)


# x = 'DeepSpark'
#
# print("'x' before passing to ScopeFunc: ", x)
# print("'y' before passing to ScopeFunc: ", y)
# print('  ')
# ScopeFunc()
# ScopeFunc(x)
# print("'x' after coming from ScopeFunc (NOTICE THE VALUE, it did not change): ", x)
# print("'y' after coming from ScopeFunc (NOTICE THE VALUE, it was changed in ScopeFunc): ", y)
# print('  ')
