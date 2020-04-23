class Person:

    def __init__(self,name):
        self.name = name
        print(name)
        print(self.name)


c = Person("yeah")
print(c.name)
