import requests as r
import json
import pandas as pd
import datetime
import time

#slack limits to 1000 message exports, need to do a week at a time
startdate = datetime.datetime(2018,4,1,0,0)
enddate = datetime.datetime(2018,4,7,11,59)

#converts start and end dates to a unix timestamp
startdateunix = str(time.mktime(startdate.timetuple()))
enddateunix = str(time.mktime(enddate.timetuple()))
token = #YourToken
channelID = #YourChannel

#accesses the api
url = 'https://slack.com/api/channels.history?token='+token+'&channel='+channelID+'&count=1000&latest='+enddateunix+'&oldest='+startdateunix+'&pretty=1'
file = r.get(url)

#reads json response element to python object
jsonfile = json.loads(file.content)
slackdata = jsonfile['messages']

#creates a pandas dataframe from the json file and filters out extraneous data
    #should probably filter out the data when formatting the json
df = pd.DataFrame.from_dict(slackdata, orient = 'columns')
text_ts = df.filter(items = ['text','ts'])
#text_ts.to_csv('test.csv')

#saves the data as a .csv
text_ts.to_csv('publishtimedata_'+str(startdate)+'.csv')

#data =[]
#for i in slackdata:
    #print(i['ts'])
    #data.append(['text']['ts'])