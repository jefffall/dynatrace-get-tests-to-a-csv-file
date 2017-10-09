import urllib
import http.client
from xml.etree import ElementTree
from datetime import datetime
import time
import sys
import os

prapi = 'datafeed-api.dynatrace.com'

def login(user, password):
    login_uri = '/publicapi/rest/v1.0/login'
    connection = http.client.HTTPSConnection(prapi)
    query_args = {}
    query_args['user'] = user
    query_args['password'] = password
    encoded_args = urllib.parse.urlencode(query_args)
    endpointurl = login_uri + '?' + encoded_args
    headers = {'Accept': "application/json", 'Accept-Encoding': "gzip,deflate"}
    try:
        connection.request("GET", endpointurl , None, headers )
        response = connection.getresponse()
        if response.status != 200:
            raise Exception( "%d (%s)" % (response.status, response.reason))
        body = response.read()
    finally:
        connection.close()
    return body.decode("utf-8")



def tests(bearerToken, testName=None, stepName=None, testType=None, status=None, url=None,  output='xml'):
    tests_uri = '/publicapi/rest/v1.0/tests'
    connection = http.client.HTTPSConnection(prapi)
    endpointurl = tests_uri
    query_args = {}
    if testName != None :
        query_args['testName'] = testName
    if stepName != None :
        query_args['stepName'] = stepName
    if testType != None :
        query_args['testType'] = testType
    if status != None :
        query_args['status'] = status
    if url != None :
        query_args['url'] = url
    if len(query_args) > 0 :
        encoded_args = urllib.parse.urlencode(query_args)
        endpointurl = endpointurl + '?' + encoded_args
    output = 'application/' + output
    headers = {'Authentication': 'bearer ' + bearerToken, 'Accept': output, 'Accept-Encoding': "gzip,deflate"}
    try:
        connection.request("GET", endpointurl , None, headers )
        response = connection.getresponse()
        if response.status != 200:
            raise Exception( "%d (%s)" % (response.status, response.reason))
        body = response.read()
    finally:
        connection.close()
    return body.decode("utf-8")



def getTestList(bearerToken, testName=None, testType=None, testStatus="ACTIVE", retType='MONITORIDS'):
#    print ("testName = %s" % testName)
    testListStr = tests(bearerToken=bearerToken, testName=testName, stepName=None, testType=testType, status=testStatus, url=None,  output='csv')
#    print ( testListStr )
    testListList = testListStr.splitlines()
   
    if retType == 'LISTOFTESTS':
        return testListList
    else:
        myList=[]
        for line in testListList:
    #        print (line)
            column = line.split(",")
            if len(column[0]) == 8 and column[0].isdigit():
                myList=myList + column[0].split()
        if myList == []:
            return ["83234"] # return a bogus MonitorID
        else:
            return myList

# For testresults pass in the monitor ID's as a LIST contained in a string

def testresults(bearerToken, start, end, time='ARRIVAL', detailLevel='TEST', output='xml', monitorIds=None):
    testresults_uri = '/publicapi/rest/v1.0/testresults'
    connection = http.client.HTTPSConnection(prapi)
    query_args = {}
    query_args['start'] = start
    query_args['end'] = end
    query_args['time'] = time
    query_args['detailLevel'] = detailLevel
    encoded_args = urllib.parse.urlencode(query_args)
    endpointurl = testresults_uri + '?' + encoded_args
    output = 'application/' + output
    headers = {'Authentication': 'bearer ' + bearerToken, 'Accept': output, 'Accept-Encoding': "gzip,deflate"}
    if monitorIds != None :
        testdata_input = ElementTree.Element('TESTDATA_INPUT')
        monitorSet = ElementTree.SubElement(testdata_input,'MonitorSet')
        for mids in monitorIds:
            monitor = ElementTree.SubElement(monitorSet, 'Monitor')
            mid = ElementTree.SubElement(monitor,'mid')
        #    print ("Monitor ID ================%s" % mids)
            mid.text = mids
    try:
        if monitorIds == None:
            connection.request("GET", endpointurl , None, headers )
        else:
            connection.request("POST", endpointurl , ElementTree.tostring(testdata_input), headers )
        response = connection.getresponse()
        if response.status != 200:
            raise Exception( "%d (%s)" % (response.status, response.reason))
        body = response.read()
    finally:
    #    print ("POST", endpointurl , ElementTree.tostring(testdata_input), headers )
        connection.close()
    return body.decode("utf-8")


    
def datetimeexample():
    d = datetime.strptime("08.03.2017 12:54:42,76", "%d.%m.%Y %H:%M:%S,%f").strftime('%s')
    d_in_ms = int(d)*1000    
    print(d_in_ms)
    print(datetime.fromtimestamp(float(d)))
    
def epochToTime(epoch):
    return datetime.fromtimestamp(float(epoch))
    


####################### Take 7 CSV list ##################################################

# This version Take 7 will attempt to gather a large time periood within a week and put it into ONE large .csv file.
# the requests will be broken down into smaller requests and appended to the file.
# All DCs' will be gathered - DC02, DC04 and DC08

def getMonitorIDTestDataFromDynatraceTake5Splunk(bearerToken, sampleWindowInMinutes, requestStartTime, requestEndTime, testStatus):
    
    if (testStatus != "ALL") and (testStatus != "ACTIVE"):
        sys.exit("testStatus must be set to ACTIVE or ALL Please look at the commandline pqrameters passed in")
    
    # all DC's
#   searchString = "DC0%BizX%Sales%Customer%Facing"

    searchString = "DC0%BizX%Sales"
    
    monitorIds = getTestList(bearerToken=bearerToken, testName=searchString, testType=None, testStatus=testStatus, retType='MONITORIDS')
    
    filename = "/Users/c/" + "DynatraceGetCSV" + "_" + requestStartTime + "_" + requestEndTime + "_" + str(datetime.now()) + ".csv"
    filenameClean = '_'.join(filename.split())
    
    target = open(filenameClean, 'w')
    
    
    d = datetime.strptime(requestStartTime, "%m.%d.%Y_%H:%M:%S").strftime('%s')
    start_epoch_time = int(d)*1000  
    d = datetime.strptime(requestEndTime, "%m.%d.%Y_%H:%M:%S").strftime('%s')
    end_epoch_time = int(d)*1000    
    
    #print (" Sample Windows size = %s minutes" % sampleWindowInMinutes)
   
    testSampleWindow = (int(sampleWindowInMinutes)) *60 *1000
    
  
    myTestResults = ""
    
    headingPrinted = "FALSE"
    
    allRecordsFoundCount = 0
    mainMinDateEpoch = 8888888888888
    mainMaxDateEpoch = 0
    
    for timeWindowStart in range(start_epoch_time, end_epoch_time, testSampleWindow):
       
        print ( "requesting from: %s to %s" % (epochToTime(int(int(timeWindowStart)/1000)), epochToTime(int(int(timeWindowStart+testSampleWindow)/1000))))
       
        myTestResults = testresults(bearerToken=bearerToken, start=timeWindowStart, end=timeWindowStart+testSampleWindow, detailLevel="STEP", output='csv', monitorIds=monitorIds)
        if len( myTestResults) < 75:
            print (" null result" )
            os.remove(filenameClean)
            sys.exit("Halted. Dynatrace API NOT returning data for selected time period. Restart with request data closer to the present date")
        print ("Length test results is %s" % len(myTestResults))
        
        time.sleep(10) # sleep 5 seconds
        
        recordCount = 0;
        retList = ["", "", ""]
        
        minDateEpoch = 8888888888888
        maxDateEpoch = 0
    
        testResultsList = myTestResults.splitlines()
    
    
    
        for record in testResultsList:
            recordCount = recordCount + 1
        #
            if len(record) > 100:
                recordList=record.split(",")
                # Count real data and not the first line which is titles
                if (recordList[0] != 'mid') and (recordList[1] != 'MBG_mid'):
        
                    if  int(recordList[2]) < minDateEpoch:
                        minDateEpoch = int(recordList[2])
                
                    if maxDateEpoch < int(recordList[3]):
                        maxDateEpoch = int(recordList[3])
                    
                        #IF this is a title - print it once only
                if (recordList[0] == 'mid') and (recordList[1] == 'MBG_mid'):
                    if headingPrinted == "FALSE":
                        headingPrinted = "TRUE"
                        
                        target.write(record + "\n")
                        target.flush()
                
                else: # Not heading so write the real data record
                    
                    target.write(record + "\n")
                    target.flush()
     
        allRecordsFoundCount = allRecordsFoundCount + recordCount
        if mainMinDateEpoch > minDateEpoch:
            mainMinDateEpoch = minDateEpoch
            
        if mainMaxDateEpoch < maxDateEpoch:
            mainMaxDateEpoch = maxDateEpoch
    target.close
   
    retList[0] = allRecordsFoundCount
    retList[1] = epochToTime(int(mainMinDateEpoch/1000))
    retList[2] = epochToTime(int(mainMaxDateEpoch/1000))
    
    return retList




#############################################################################################################

# TODO:   Inputs are readable GMT time.

 
    
if __name__ == "__main__":
    
    
    
    # get login
    bearerToken = login(sys.argv[1], sys.argv[2])
    
   
    t1=datetime.now()
    runTimeBegin = epoch_time = int(time.time())

   
  
    # make monitor ID's into a LIST split on space
#    monitorIDs =  sys.argv[3].split(" ")
#   datecenter = sys.argv[3] 
    sampleWindowInMinutes = sys.argv[3]
   
    # get start and end into descriptive variables
    start_time = sys.argv[4]
    end_time = sys.argv[5]
    
    testStatus = sys.argv[6]
    
    print ("%s START:  SWIN: %s BEG: %s END: %s" % ( (epochToTime(runTimeBegin)), sys.argv[3], start_time, end_time  ) )
    
#  
    retVals = getMonitorIDTestDataFromDynatraceTake5Splunk(bearerToken=bearerToken, sampleWindowInMinutes=sampleWindowInMinutes, requestStartTime=start_time, requestEndTime=end_time, testStatus=testStatus)
    runTimeEnd = epoch_time = int(time.time())
    timeDelta = runTimeEnd - runTimeBegin
    t2=datetime.now()
    elapsed = t2-t1
    
    records=retVals[0]
    mindate = retVals[1]
    maxdate = retVals[2]
   
    
    print ("%s FINISH: SWIN: %s BEG: %s END: %s RUNBEG: %s RUNEND: %s ELAPSED: %s RECORDS: %s MINDATE: %s MAXDATE: %s" % ( (epochToTime(runTimeEnd)), sys.argv[3], start_time, end_time, epochToTime(int(runTimeBegin)),
                                                                                          epochToTime(int(runTimeEnd)), elapsed, records, mindate, maxdate) )
    
