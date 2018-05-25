import requests as r
import json
import pandas as pd
import datetime
import time

#slack limits exports to 1000 messages at a time. Start and end dates are spaced a week apart to stay under that limit per pull.
startdate1 = datetime.datetime(2018,5,1,0,0)
enddate1 = datetime.datetime(2018,5,6,23,59)

startdate2 = datetime.datetime(2018,5,7,0,0)
enddate2 = datetime.datetime(2018,1,13,23,59)

startdate3 = datetime.datetime(2018,5,14,0,0)
enddate3 = datetime.datetime(2018,5,20,23,59)


#create list of all start and end dates to loop through
startdates = (startdate1,
             startdate2,
             startdate3)

enddates = (enddate1,
            enddate2,
            enddate3)

#function that pulls data from slack and creates a pandas dataframe. 
def datapull(startdate,enddate):
    startdateunix = str(time.mktime(startdate.timetuple()))
    enddateunix = str(time.mktime(enddate.timetuple()))
    token = 
    channelID = 
    url = 'https://slack.com/api/channels.history?token='+token+'&channel='+channelID+'&count=1000&latest='+enddateunix+'&oldest='+startdateunix+'&pretty=1'
    file = r.get(url)
    jsonfile = json.loads(file.content)
    slackdata = jsonfile['messages']
    df = pd.DataFrame.from_dict(slackdata, orient = 'columns')
    global text_ts
    text_ts = df.filter(items = ['text','ts'])
    

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

#create new columns containing the post URL and the clickability ID
final_df['Post_URL'] = final_df['text'].str.extract('(http.*\.html)')
final_df['Clicka_ID'] = final_df['text'].str.extract('(\d{9})')
final_df['DateTimeEST'] = pd.to_datetime(final_df['ts'],unit = 's').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

print(final_df.head())
#final_df.to_csv('publishtimedata_'+str(startdate1)+'_'+str(enddate3)+'.csv')