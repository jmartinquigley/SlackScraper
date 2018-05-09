import requests as r
import json
import pandas as pd
import datetime
import time

#slack limits exports to 1000 messages at a time. Start and end dates are spaced a week apart to stay under that limit per pull.
startdate1 = datetime.datetime(2018,1,1,0,0)
enddate1 = datetime.datetime(2018,1,6,11,59)

startdate2 = datetime.datetime(2018,7,1,0,0)
enddate2 = datetime.datetime(2018,1,13,11,59)

startdate3 = datetime.datetime(2018,1,14,0,0)
enddate3 = datetime.datetime(2018,1,20,11,59)

startdate4 = datetime.datetime(2018,1,21,0,0)
enddate4 = datetime.datetime(2018,1,27,11,59)

startdate5 = datetime.datetime(2018,1,28,0,0)
enddate5 = datetime.datetime(2018,2,3,11,59)

startdate6 = datetime.datetime(2018,2,4,0,0)
enddate6 = datetime.datetime(2018,2,10,11,59)

startdate7 = datetime.datetime(2018,2,11,0,0)
enddate7 = datetime.datetime(2018,2,17,11,59)

startdate8 = datetime.datetime(2018,2,18,0,0)
enddate8 = datetime.datetime(2018,2,24,11,59)

startdate9 = datetime.datetime(2018,2,25,0,0)
enddate9 = datetime.datetime(2018,3,3,11,59)

startdate10 = datetime.datetime(2018,3,4,0,0)
enddate10 = datetime.datetime(2018,3,10,11,59)

startdate11 = datetime.datetime(2018,3,11,0,0)
enddate11 = datetime.datetime(2018,3,17,11,59)

startdate12 = datetime.datetime(2018,3,18,0,0)
enddate12 = datetime.datetime(2018,3,24,11,59)

startdate13 = datetime.datetime(2018,3,25,0,0)
enddate13 = datetime.datetime(2018,3,31,11,59)

startdate14 = datetime.datetime(2018,4,1,0,0)
enddate14 = datetime.datetime(2018,4,7,11,59)

startdate15 = datetime.datetime(2018,4,8,0,0)
enddate15 = datetime.datetime(2018,4,14,11,59)

startdate16 = datetime.datetime(2018,4,15,0,0)
enddate16 = datetime.datetime(2018,4,21,11,59)

startdate17 = datetime.datetime(2018,4,22,0,0)
enddate17 = datetime.datetime(2018,4,28,11,59)

startdate18 = datetime.datetime(2018,4,29,0,0)
enddate18 = datetime.datetime(2018,5,5,11,59)

#create list of all start and end dates to loop through
startdates = (startdate1,
             startdate2,
             startdate3,
             startdate4,
             startdate5,
             startdate6,
             startdate7,
             startdate8,
             startdate9,
             startdate10,
             startdate11,
             startdate12,
             startdate13,
             startdate14,
             startdate15,
             startdate16,
             startdate17,
             startdate18)

enddates = (enddate1,
            enddate2,
            enddate3,
            enddate4,
            enddate5,
            enddate6,
            enddate7,
            enddate8,
            enddate9,
            enddate10,
            enddate11,
            enddate12,
            enddate13,
            enddate14,
            enddate15,
            enddate16,
            enddate17,
            enddate18)

#function that pulls data from slack and creates a pandas dataframe. 
def datapull(startdate,enddate):
    startdateunix = str(time.mktime(startdate.timetuple()))
    enddateunix = str(time.mktime(enddate.timetuple()))
    token = 'TOKEN'
    channelID = 'CHAN_ID'
    url = 'https://slack.com/api/channels.history?token='+token+'&channel='+channelID+'&count=1000&latest='+enddateunix+'&oldest='+startdateunix+'&pretty=1'
    file = r.get(url)
    jsonfile = json.loads(file.content)
    slackdata = jsonfile['messages']
    df = pd.DataFrame.from_dict(slackdata, orient = 'columns')
    global text_ts
    text_ts = df.filter(items = ['text','ts'])
    

#to do: need to create a loop which pulls the data from slack and puts it in a dataframe. Another nice to have would be converting the unix timestamp('ts') in the dataframe to EST.

# emtpy list to hold dataframes
dfs = []

#loop thru date lists & store text_ts objects in list    
for i in range(len(startdates)):
    datapull(startdates[i], enddates[i])
    dfs.append(text_ts)
 
# concatenate dfs into single final df   
for i in range(len(dfs)):
    if i == 0: 
        final_df = dfs[i]
        
    else:
        temp_df = dfs[i]
        final_df = pd.concat([final_df, temp_df], ignore_index=True)

final_df.to_csv('publishtimedata_20181010-20180505.csv')