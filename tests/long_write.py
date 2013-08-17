import sys
import time

f = open (sys.argv[1], 'w')
while True:
    f.write ("test")
    f.flush ()
    time.sleep (0.01)
