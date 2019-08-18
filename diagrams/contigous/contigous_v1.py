import matplotlib.pyplot as plt
import numpy as np

#x = np.random.normal(size = 1000)
#plt.hist(x, density=True, bins=30) # density
#plt.ylabel('Probability')
#plt.show()

x = np.arange(2)
plt.title("Contigous hystory constraint v2 performance")
barlist = plt.bar(x, height= [16,20]) 
plt.xticks(x, ['success','invalid'])

plt.ylabel(ylabel = "Time (ms)")
plt.xlabel(xlabel = "Result of the check")

barlist[0].set_color('g')
barlist[1].set_color('r')
plt.show()