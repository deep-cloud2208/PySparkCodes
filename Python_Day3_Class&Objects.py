#-------------------------------------------------------------------------------
# Classes and Objects
#-------------------------------------------------------------------------------
class MyClass:
    clsVar1 = "First Class Variable"
    clsVar2 = "Second Class Variable"

    print(clsVar1)
    print(clsVar2)

    def clsMethod(self,*args):
        result = 0
        for i in args:
            result = result + i
        return result


MyObj1 = MyClass()
MyObj2 = MyClass()

MyObj1.clsVar1 = "Intellipaat"
MyObj2.clsVar1 = "PySpark"

print(MyObj1.clsVar1)
print(MyObj2.clsVar1)
print(MyObj1.clsMethod(2,3,4,5))
print(MyObj2.clsMethod(10,20,30,40))

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