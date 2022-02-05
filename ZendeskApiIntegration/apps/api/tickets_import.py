"""
This module is designed to uplaod the ticket data.
Contains a logic to process a data
Contains a logic generate a payload
"""
from requests.auth import HTTPBasicAuth
from ZendeskApiIntegration.apps.util.utility import *
from ZendeskApiIntegration.apps.util.constant import *



class TicketsImport:
    def __init__(self,configFilePath):
        self.configFilePath=configFilePath
        self.config=getPropertyfromConfig(self.configFilePath)

    def uploadticketData(self):
        ticketDataDf = getinputDatafromCsv(self.config['TicketDataLocation'])
        ticketDataDf['assignee_id'].fillna(default_assign_id,inplace=True)
        ticketDataDf['status']=ticketDataDf['status'].apply(lambda x: statusMap[x] if x in statusMap.keys() else x)
        listTicketDataDf = getSplittedDf(ticketDataDf)
        for ticketdata in listTicketDataDf:
            tickets = []
            ticketdata.apply(lambda row : tickets.append(self.createJson(row)),axis=1)
            data = {'tickets': tickets}
            auth=HTTPBasicAuth(self.config['UserName'], self.config['Password'])
            resp,status=invokeApi(self.config['TicketImportUrl'], data ,auth)
            print(resp,status)

    def createJson(self,row):
        record={}
        record['assignee_id']=int(float((row['assignee_id'])))
        record['created_at']=str(row['created_at'])
        record['subject']=row['subject']
        record['description']=row['description']
        record['status']=row['status']
        record['submitter_id']=int(row['submitter_id'])
        record['requester_id']=int(row['requester_id'])
        record['due_at']=''if pd.isna(row['due_at']) else str(row['due_at'])
        _tag='' if pd.isna(row['tags']) else row['tags'].replace("'",'')
        record['tags']=['Darshan']+_tag.strip('][').split(', ')

        custom_data={'about':'' if pd.isna(row['about']) else row['about'],
                     'business name':'' if pd.isna(row['business name']) else row['business name'],
                     'dept':'' if pd.isna(row['dept']) else row['dept'],
                     'product information':'' if pd.isna(row['product information']) else row['product information']}
        record['custom_fields']=[custom_data]

        return record

