import argparse
from  ZendeskApiIntegration.apps.tickets_import import TicketsImport
from ZendeskApiIntegration.apps.user_import import  UsersImport
def main():
    parser = argparse.ArgumentParser(description='Zendesk API Integration Inputs.')
    parser.add_argument('configLocation')
    parser.add_argument('app')
    args = parser.parse_args()
    if(args.app=='userimport'):
        userimport=UsersImport(args.configLocation)
        userimport.uploadUserData()
    else:
        ticketimport=TicketsImport(args.configLocation)
        ticketimport.uploadticketData()


if __name__ == '__main__':
    main()




