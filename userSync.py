# coding: utf-8
import json
import tableauserverclient as TSC
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading

#Tableau
tableauId = ""
tableauPass = ""
tableauURL = "https://"

#Spreadsheet
sheetId = ""
sheetName = ""

def lambda_handler(event, context):
    results = []
    results.append(["ID","ROLE","SITE","MAXROLE"])
    getSites(results)

    credentials = ServiceAccountCredentials.from_json_keyfile_name('auth.json', 'https://spreadsheets.google.com/feeds')
    client = gspread.authorize(credentials)
    wb = client.open_by_key(sheetId)
    ws = wb.worksheet(sheetName)
    ws.clear()
    roleMaster = {}
    roleMaster["ServerAdministrator"] = "Creator"
    roleMaster["SiteAdministratorCreator"] = "Creator"
    roleMaster["Creator"] = "Creator"
    roleMaster["SiteAdministratorExplorer"] = "Explorer"
    roleMaster["ExplorerCanPublish"] = "Explorer"
    roleMaster["Explorer"] = "Explorer"
    roleMaster["Viewer"] = "Viewer"
    roleMaster["Unlicensed"] = "Unlicensed"
    roleMaster[""] = "Unlicensed"
    
    cell_list = ws.range('A1:D'+str(len(results)*4+1))
    users = {}
    for key,row in enumerate(results):
        cell_list[(key*4)+0].value = row[0]
        cell_list[(key*4)+1].value = row[1]
        cell_list[(key*4)+2].value = row[2]
        
        if key != 0:
            if not users.get(row[0]):
                users[row[0]] = roleMaster[row[1]]
            elif roleMaster[users.get(row[0])] == "Creator" or roleMaster[row[1]] == "Creator":
                users[row[0]] = "Creator"
            elif roleMaster[users.get(row[0])] == "Explorer" or roleMaster[row[1]] == "Explorer":
                users[row[0]] = "Explorer"
            elif roleMaster[users.get(row[0])] == "Viewer" or roleMaster[row[1]] == "Viewer":
                users[row[0]] = "Viewer"
            else:
                users[row[0]] = "Unlicensed"
        else:
            cell_list[(key*4)+3].value = row[3]
    for key,row in enumerate(results):
        if key != 0:
            cell_list[(key*4)+3].value = users[row[0]]
    ws.update_cells(cell_list)

    return {'statusCode': 200,'headers': {"Content-Type": "text/html"},'body': "ok"}

def getSite(site,results):
    request_options = TSC.RequestOptions(pagesize=1000)
    tableau_auth_site = TSC.TableauAuth(tableauId, tableauPass,site.content_url)
    server = TSC.Server(tableauURL)
    server.version = "3.2"
    with server.auth.sign_in(tableau_auth_site):
        all_users, pagination_item = server.users.get(request_options)
        for user in all_users:
            results.append([user.name,user.site_role,site.content_url])
                
def getSites(results):
    tableau_auth = TSC.TableauAuth(tableauId, tableauPass)
    server = TSC.Server(tableauURL)
    server.version = "3.2"
    request_options = TSC.RequestOptions(pagesize=1000)
    threadlist = list()
    with server.auth.sign_in(tableau_auth):
        all_sites, pagination_item = server.sites.get(request_options)
        for site in all_sites:
            if site.state == "Active" and site.content_url != "":
                arg = site
                thread = threading.Thread(target=getSite, args=([site,results]), name=site.content_url)
                threadlist.append(thread)
        for thread in threadlist:
            thread.start()
        for thread in threadlist:
            thread.join()
