#-------------------------------------------------------------------------------
# Classes and Objects
#-------------------------------------------------------------------------------
# class MyClass:
#     clsVar1 = "First Class Variable"
#     clsVar2 = "Second Class Variable"
#
#     print(clsVar1)
#     print(clsVar2)
#
#     def clsMethod(self,*args):
#         result = 0
#         for i in args:
#             result = result + i
#         return result
#
#
# MyObj1 = MyClass()
# MyObj2 = MyClass()
#
# MyObj1.clsVar1 = "Intellipaat"
# MyObj2.clsVar1 = "PySpark"
#
# print(MyObj1.clsVar1)
# print(MyObj2.clsVar1)
# print(MyObj1.clsMethod(2,3,4,5))
# print(MyObj2.clsMethod(10,20,30,40))

# MyObj.clsVar1 = "Obj Var1"
# MyObj.clsVar2 = "Obj Var2"
# sum = MyObj.clsMethod(2,3,4,5)

# print(MyObj.clsVar1)
# print(MyObj.clsVar2)
# print(sum)
# print(MyClass.__dict__)

#-------------------------------------------------------------------------------
# Class Variables
#-------------------------------------------------------------------------------
# Step 1 - Class
# - Define a class
# - Define __init__ method
# - What is "self"
# - Class variables - Can be accessed from an instance as well. Can be modified
#   from either using Class reference or using instance reference. but there's
#   a difference


# Step 2 - Class Methods
# i) How 'self' is used in the method
# ii)
# TO BE COMPLETED LATER ON
#-------------------------------------------------------------------------------



#-------------------------------------------------------------------------------
# Lambda Functions (Also called Anonymous Functions or Lambda Expressions)
# Few traits:
# - Cannot be used to define complex logic. Cannot have multiple lines or else
#   it will give error.
# - Mostly used in Data processing frameworks like Spark.
# - RETURN statement is not needed.
# - Following is the structure to define Lambda function
#   lambda x,y,z: <expression>.
# - To call a Lambda function use the following syntax
#   i) Define the function as a variable > f = lambda x: x+1
#   ii) Call the variable with argument > f(3)
#-------------------------------------------------------------------------------

# def multiply_by_three(x):
#     return 3*x + 1
#
# x = multiply_by_three(5)
# print(x)
#
# y = lambda x: 3*x+1
# z = y(5)
# print(z)

# Quadratic equation using Lambda - ax^2 + bx + c
#-------------------------------------------------------------------------------
# def quad(a,b,c,x):
#     return a*x**2 + b*x + c
#
# y = quad(2,4,5,1)
# print(y)

# y = lambda a,b,c,x: a*x**2 + b*x + c
# var = y(2,4,5,1)
# print(var)

# find out the length of strings in the lastname
# scientists = ["Isaac Newton", "Albert Einstein", "Abdul Kalam", "Jagdish Bose", "Stephen Hawking"]

# def get_last_name_len(name):
#     lname = name.split(' ')[1].lower()
#     lname_len = len(lname)
#     return lname_len

# length_lambda = lambda x: x.split(' ')[1]

# for i in scientists:
    # Write the same using Lambda functions here
    # length= get_last_name_len(i)
    # length = len(length_lambda(i))
    # print("LName strings for {} is : {}".format(i,length))


#-------------------------------------------------------------------------------
# Python Modules
# SYS.PATH
#-------------------------------------------------------------------------------
# import sys
# from Python_Day3_Functions import myFunc, TechStack
# # import Python_Day3_Functions
#
# sys.path.append('/your/program/path')

# Environment Variable PYTHONPATH

# print(sys.path)

# x = myFunc(20,70)
# y = TechStack
# print(x,y)



















