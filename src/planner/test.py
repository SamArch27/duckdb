import math

# SELECT * FROM ... WHERE udf(x,y)

# Create a new UDF which just has
# the control flow information

# JIT this
def model_udf(x,y):
    udf(x,y)
    return True

# Output
# GUARD(x < 50)
# counter++

def udf(x,y):
    if x < 20:
        z = x ** 2
    elif (x < 50):
        z = z + 1
    else:
        for i in range(x):
            z = math.pow(math.sqrt(y), i) + z
    return z

# GUARD(x < 20)
# z = x ** 2
# RETURN z

# GUARD(x >= 20)
# loop(...)
# RETURN z

# Aggressive Dead Code Elimination