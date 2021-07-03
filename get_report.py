import re
import os
import sys
import vtuber_dict as vd



top1 = ["선장"]
vtuber_dict = vd.vtuber

vtuber_posts = {}
# for vtuber_dict = {}

# for vtuber in top10:
#     vtuber_posts[vtuber] = []

with open('./new/ai.txt', 'r', encoding='cp949') as f:
    f2 = open('./new/1.txt', 'w')
    line = f.readline()
    while line:
        # print(line)
        for vtuber in top1:
            for nickname in vtuber_dict[vtuber]:
                if line.find(nickname) != -1 :
                    f2.write(line)
                    break
        line = f.readline()
