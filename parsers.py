 #-*- coding: utf-8 -*-
import os
import sys
import pandas as pd
import numpy as np
import vtuber_dict as vd
import json
import calendar
import collections
from datetime import datetime
gall = 'kizunaai';year=datetime.now().year; month =datetime.now().month-1 # 갤러리, 연, 월
baseloc=os.path.abspath('./%s/%s/%s/' % (gall,year,month))

old_month = month - 1
old_year = year
if month == 1:
    old_month = 12
    old_year = year - 1
oldloc=os.path.abspath('./%s/%s/%s/' % (gall,old_year,old_month))

totaltag = []
lastday = calendar.monthrange(year,month)[1]
for beg in range(1,lastday+1):
    with open(baseloc+'word/'+str(beg)+'_day.json', 'r') as f:totaltag.append(dict(json.load(f)))
    with open(baseloc+'word/'+str(beg)+'_night.json', 'r') as f:totaltag.append(dict(json.load(f)))
counter = collections.Counter()
for d in totaltag:counter.update(d)
tagsdict = dict(counter)

def parser(datafolder, vtuber_dict, output_filename, savefolder=baseloc, encoder='utf8'):
    log = []
    if not os.path.exists(savefolder):
        os.makedirs(savefolder)
    
    writer_path = os.path.join(savefolder, output_filename)
    
    for vtuber in vtuber_dict:
        total_call = 0
        tags = {}
        for nickname in vtuber_dict[vtuber]:
            if nickname in tagsdict.keys():
                count = tagsdict[nickname]
                total_call += count
                tags[nickname] = count
            else:
                tags[nickname] = 0
        
        sorted_dict = sorted(tags.items(), key=(lambda x: x[1]), reverse = True)
        summary = ""
        for nickname, values in sorted_dict:
            summary += f"{nickname}({values})+"
        summary = summary[:-1]
        
        log.append([vtuber, summary, total_call])

    new_data = pd.DataFrame(log, columns=["Vtuber","단어(언급 수)", "총 언급 횟수"])
    new_data.sort_values(by=['총 언급 횟수'], ascending=False, inplace=True)
    
    
    new_data.index = np.arange(1, len(new_data) + 1)
    try:
        old_data = pd.read_csv(os.path.join(oldloc, output_filename))
    except:
        old_data = pd.DataFrame()

    
    standard = new_data.iat[0,2]
    barlength = 18.56 / standard
    for i in range(1,10):
        print(f"{i+1}번째 바 길이: {new_data.iat[i,2]*barlength}")
    out_vtuber_df= old_data.head(n=10)[~old_data["Vtuber"].head(n=10).isin(new_data["Vtuber"].head(n=10))]
    
    print(f'Top10에서 사라진: {new_data[new_data["Vtuber"].isin(out_vtuber_df["Vtuber"])]}')

    compared_ranks = []
    for idx, member in new_data.iterrows():
        vtuber = member["Vtuber"]
        try:
            old_rank = old_data[old_data["Vtuber"] == vtuber].index[0]+1
        except:
            old_rank = "NA"

        if old_rank == "NA":
            compared_rank = f"NEW"
        elif idx > old_rank:
            compared_rank = f"▼{idx-old_rank}"
        elif idx < old_rank:
            compared_rank = f"▲{old_rank-idx}"
        elif idx == old_rank:
            compared_rank = f"■-"
        else:
            compared_rank="Error"
        compared_ranks.append(compared_rank)

    new_data["전월 대비 순위"]=compared_ranks

    new_data.to_csv(writer_path, sep=',', index=True)


if __name__ == "__main__":
    # run preprocess with argv
    parser(baseloc, vd.nijisanji, 'sum_niji.csv')
    parser(baseloc, vd.hololive, 'sum_holo.csv')
    parser(baseloc, vd.vtuber, 'sum_ai.csv')
    
    print("Parse done.")