import sys
import re
f = open('in.txt', 'r')
for line in f:
    s=line.split()
    if len(s)==4:
        word1=s[0]
        word2=s[1]
        word3=s[2]
        n3=int(s[3])
    elif len(s)==3:
        n2=int(s[2])
    else:
        print("input error")
        break

print(word1,word2,word3,n3,n2,n3/n2)
f.close
