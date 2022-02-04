"""
This module is designed to uplaod the user data.
Contains a logic to process a data
Contains a logic generate a payload
"""
from requests.auth import HTTPBasicAuth
from ZendeskApiIntegration.apps.utility import *

class UsersImport:
    def __init__(self,configFilePath):
        self.configFilePath=configFilePath
        self.config=getPropertyfromConfig(self.configFilePath)

    def uploadUserData(self):

        userDataDf=getinputDatafromCsv(self.config['UserDataLocation'])
        userDf = self.handleDuplicateUsersEmail(userDataDf)
        listUserDataDf=getSplittedDf(userDf)
        for userdata in listUserDataDf:
            users = []
            for index, row in userdata.iterrows():
                users.append(self.createJson(row))
            data = {'users': users}
            auth=HTTPBasicAuth(self.config['UserName'], self.config['Password'])
            resp,status=invokeApi(self.config['UserImportUrl'], data ,auth)
            print(resp,status)

    def createJson(self,row):
        record={}
        record['external_id']=str(row['id'])
        record['name']=row['name']
        record['email']='' if pd.isna(row['email']) else row['email']
        organization_id=row['organization_id']
        organization_id = organization_id.replace("'", '')
        organization_list=organization_id.strip('][').split(', ')
        record['organization_id']=int(float(organization_list[0]))
        record['role']=row['role']
        record['active']=row['active']
        record['notes']='' if pd.isna(row['notes']) else row['notes']
        _tag='' if pd.isna(row['tags']) else row['tags'].replace("'",'')
        record['tags']=['Darshan']+_tag.strip('][').split(', ')
        custom_data = {'group': '' if pd.isna(row['group']) else row['group'],
                       'api_subscription': '' if pd.isna(row['api_subscription']) else row['api_subscription'],
                       'promotion_code': '' if pd.isna(row['promotion_code']) else row['promotion_code']
                       }
        record['custom_fields'] = [custom_data]
        return record

    def handleDuplicateUsersEmail(self,userDf):

        userDf = userDf.drop_duplicates(
            subset=['email', 'name', 'role'],
            keep='last').reset_index(drop=True)
        return userDf
