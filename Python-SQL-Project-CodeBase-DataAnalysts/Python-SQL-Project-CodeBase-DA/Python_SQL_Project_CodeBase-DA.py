import argparse as agp
import getpass
import os

from myTools import MSSQL_DBConnector as mssql
from myTools import DBConnector as dbc
import myTools.ContentObfuscation as ce


try:
    import pandas as pd
except:
    mi.installModule("pandas")
    import pandas as pd



def printSplashScreen():
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")



def processCLIArguments()-> dict:
    
    retParametersDictionary:dict = None
    
    dbpassword:str = ''
    obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

    try:
        argParser:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

        argParser.add_argument("-n", "--DSN", dest="dsn", \
                                action='store', default= None, help="Sets the SQL Server DSN descriptor file - Take precedence over all access parameters", type=str)

        argParser.add_argument("-s", "--DBServer", dest="dbserver", \
                                action='store', help="Sets the SQL Server DB Server Address", type=str)
        
        argParser.add_argument("-d", "--DBName", dest="dbname", \
                                action='store', help="Sets the SQL Server DB Name", type=str)

        argParser.add_argument("-u", "--DBUser", dest="dbusername", \
                                action='store', help="Sets the SQL Server DB Username", type=str)

        argParser.add_argument("-p", "--DBUserPassword", dest="dbuserpassword", \
                                action='store', help="Sets the SQL Server DB User Password", type=str)

        argParser.add_argument("-t", "--UseTrustedConnection", dest="trustedmode", \
                                action='store', default=False, \
                                help="Sets the SQL Server connection in Trusted Connection mode (default = False)", type=bool)

        argParser.add_argument("-v", "--TargetViewName", dest="viewname", \
                                action='store', default="dbo.vw_AllSurveyData", \
                                help="Sets the SQL Server target view name (this can be SCHEMA.VIEWNAME) - (default = dbo.vw_AllSurveyData)", type=str)

        argParser.add_argument("-f", "--SerialisedPersistenceFilePath", dest="persistencefilepath", \
                                action='store', default=os.getcwd() + os.sep + "lastKnownSurveyStructure.pkl", \
                                help="Sets the Persistence File Path (default = script current directory given by os.getcwd())", type=str)

        argParser.add_argument("-r", "--ResultsFilePath", dest="resultsfilepath", \
                                action='store', default=os.getcwd() + os.sep + "results.csv", \
                                help="Sets the Results (CSV) File Path (default = script current directory given by os.getcwd())", type=str)
       
        argParsingResults:agp.Namespace = argParser.parse_args()
        
        if(argParsingResults.dbserver is None):
            raise Exception("You must provide a SQL Server Address using the -s or --DBServer CLI argument")

        if(argParsingResults.dbname is None):
            raise Exception("You must provide a target database name using the -d or --DBName CLI argument")

        if(argParsingResults.trustedmode == False):   
            if(argParsingResults.dbusername is None):
                raise Exception("You must provide a DB user account name using the -u or --DBUser CLI argument (or use TrustedConnection Mode)")

            #Password should not be provided directly in cleartext on the CLI
            if(argParsingResults.dbuserpassword is None):
                dbpassword = obfuscator.obfuscate(getpass.getpass('Please type the DB user password (no echo): '))
            else:
                dbpassword = obfuscator.obfuscate(argParsingResults.dbuserpassword)
            
        else:
            if(argParsingResults.dbusername is not None):
                raise Exception("You are using the TrustedConnection Mode yet providing DB user name: this will be disregarded in TrustedConnection")

        retParametersDictionary = {
                    "dsn" : argParsingResults.dsn,        
                    "dbserver" : argParsingResults.dbserver,
                    "dbname" : argParsingResults.dbname,
                    "dbusername" : argParsingResults.dbusername,
                    "dbuserpassword" : dbpassword,
                    "trustedmode" : argParsingResults.trustedmode,
                    "viewname" : argParsingResults.viewname,
                    "persistencefilepath": argParsingResults.persistencefilepath,
                    "resultsfilepath" : argParsingResults.resultsfilepath
                }

    except Exception as e:
        print("Command Line arguments processing error: " + str(e))

    return retParametersDictionary

def getSurveyStructure(connector: mssql.MSSQL_DBConnector) -> pd.DataFrame:
    
    surveyStructResults = None

    if(connector is not None):

        if(connector.IsConnected == True):

            strQuerySurveyStruct = """
            SELECT  SurveyId, QuestionId
            FROM    SurveyStructure
            ORDER BY    SurveyId, QuestionId """

            try:
                surveyStructResults:pd.DataFrame = connector.ExecuteQuery_withRS(strQuerySurveyStruct)
            except Exception as excp:
                raise Exception('GetSurveyStructure(): Cannot execute query').with_traceback(excp.__traceback__)

        else:
            raise Exception("GetSurveyStructure(): no Database connection").with_traceback(excp.__traceback__)

    else:
        raise Exception("GetSurveyStructure(): Database connection object is None").with_traceback(excp.__traceback__)

    return surveyStructResults



def doesPersistenceFileExist(persistenceFilePath: str)-> bool:

    success = True

    try:
        file = open(persistenceFilePath)
        file.close()

    except FileNotFoundError:
        success = False

    return success



def isPersistenceFileDirectoryWritable(persistenceFilePath: str)-> bool:
    success = True
    if(os.access(os.path.dirname(persistenceFilePath), os.W_OK) == False):
        success = False

    return success


def compareDBSurveyStructureToPersistenceFile(surveyStructResults:pd.DataFrame, persistenceFilePath: str) -> bool:
    
    same_file = False
   
    try:
        unpickled_persistanceFileDF = pd.read_csv(persistenceFilePath)
        if(surveyStructResults.equals(unpickled_persistanceFileDF) == True):
            same_file = True

    except Exception as excp:
       raise Exception("compareDBSurveyStructureToPersistenceFile(): Couldn't read (unpickle) the persistence file").with_traceback(excp.__traceback__)

    return same_file



def getAllSurveyDataQuery(connector: dbc.DBConnector) -> str:

    #IN THIS FUNCTION YOU MUST STRICTLY CONVERT THE CODE OF getAllSurveyData written in T-SQL, available in Survey_Sample_A19 and seen in class
    # Below is the beginning of the conversion
    # The Python version must return the string containing the dynamic query (as we cannot use sp_executesql in Python!)
    strQueryTemplateForAnswerColumn: str = """COALESCE( 
				( 
					SELECT a.Answer_Value 
					FROM Answer as a 
					WHERE 
						a.UserId = u.UserId 
						AND a.SurveyId = <SURVEY_ID> 
						AND a.QuestionId = <QUESTION_ID> 
				), -1) AS ANS_Q<QUESTION_ID> """ 


    strQueryTemplateForNullColumnn: str = ' NULL AS ANS_Q<QUESTION_ID> '

    strQueryTemplateOuterUnionQuery: str = """ 
			SELECT 
					UserId 
					, <SURVEY_ID> as SurveyId 
					, <DYNAMIC_QUESTION_ANSWERS> 
			FROM 
				[User] as u 
			WHERE EXISTS 
			( \
					SELECT * 
					FROM Answer as a 
					WHERE u.UserId = a.UserId 
					AND a.SurveyId = <SURVEY_ID> 
			) 
	"""

   # strCurrentUnionQueryBlock: str = ''
 
    strFinalQuery: str = ''

    #MAIN LOOP, OVER ALL THE SURVEYS

    # FOR EACH SURVEY, IN currentSurveyId, WE NEED TO CONSTRUCT THE ANSWER COLUMN QUERIES
	#inner loop, over the questions of the survey

    # Cursors are replaced by a query retrived in a pandas df
    surveyQuery:str = 'SELECT SurveyId FROM Survey ORDER BY SurveyId' 
    surveyQueryDF:pd.DataFrame = connector.ExecuteQuery_withRS(surveyQuery)

    #CARRY ON THE CONVERSION

    # LOOP over surveyId and use Pandas DataFrame.iterrows() to iterate over data frame

    for n,data in surveyQueryDF.iterrows():
        currentSurveyId = data['SurveyId']
        print(currentSurveyId)
        strCurrentUnionQueryBlock: str = ''
       
        currentQuestionCursorStr:str = """SELECT * FROM ( SELECT SurveyId, QuestionId, 1 as InSurvey FROM SurveyStructure WHERE SurveyId = %s UNION SELECT %s as SurveyId,Q.QuestionId,0 as InSurvey FROM Question as Q WHERE NOT EXISTS(SELECT *FROM SurveyStructure as S WHERE S.SurveyId = %s AND S.QuestionId = Q.QuestionId )) as t ORDER BY QuestionId; """ % (currentSurveyId,currentSurveyId,currentSurveyId)
        currentQuestionCursorDF:pd.DataFrame = connector.ExecuteQuery_withRS(currentQuestionCursorStr)
       
        strColumnsQueryPart:str='';
        for m,currentQData in currentQuestionCursorDF.iterrows():

            currentInSurvey = currentQData['InSurvey']
            currentSurveyIdInQuestion = currentQData['SurveyId']
            currentQuestionID = currentQData['QuestionId']
            
            if currentInSurvey == 0 :
                strColumnsQueryPart= strColumnsQueryPart + strQueryTemplateForNullColumnn.replace('<QUESTION_ID>',str(currentQuestionID))
            else :
                strColumnsQueryPart= strColumnsQueryPart + strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>',str(currentQuestionID))
            
            if m != len(currentQuestionCursorDF.index) - 1 :
                strColumnsQueryPart = strColumnsQueryPart + ', '
      
       
        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock + strQueryTemplateOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>',str(strColumnsQueryPart))
        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId)) 

        strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock
        if n != len(surveyQueryDF.index)-1 :
            strFinalQuery = strFinalQuery + ' UNION '

    return strFinalQuery



def refreshViewInDB(connector: dbc.DBConnector, baseViewQuery:str, viewName:str)->None:
    
    if(connector.IsConnected == True):
        
        refreshViewQuery = "CREATE OR ALTER VIEW " + viewName + " AS " + baseViewQuery

        try:
            cursor = connector._dbConduit.cursor()
            cursor.execute(refreshViewQuery)
            connector._dbConduit.commit()

        except Exception as excp:
            raise excp

    else:
        raise Exception('Cannot refresh view, connector object not connected to DB')


def surveyResultsToDF(connector: dbc.DBConnector, viewName:str)->pd.DataFrame:
    
    results:pd.DataFrame = None

    if(connector.IsConnected == True):
        
        selectAllSurveyDataQuery = "SELECT * FROM " + viewName + " ORDER BY UserId, SurveyId"
        try:
            results = connector.ExecuteQuery_withRS(selectAllSurveyDataQuery)

        except Exception as excp:
            raise excp

    else:
        raise Exception('Cannot refresh view, connector object not connected to DB')

    return results


def main():
    
    cliArguments:dict = None

    printSplashScreen()

    try:
        cliArguments = processCLIArguments()
    except Except as excp:
        print("Exiting")
        return

    if(cliArguments is not None):
        
        #if you are using the Visual Studio Solution, you can set the command line parameters within VS (it's done in this example)
        #For setting your own values in VS, please make sure to open the VS Project Properties (Menu "Project, bottom choice), tab "Debug", textbox "Script arguments"
        #If you are trying this script outside VS, you must provide command line parameters yourself, i.e. on Windows
        #python.exe Python_SQL_Project_Sample_Solution --DBServer <YOUR_MSSQL> -d <DBName> -t True
        #See the processCLIArguments() function for accepted parameters

        try:
            connector = mssql.MSSQL_DBConnector(DSN = cliArguments["dsn"], dbserver = cliArguments["dbserver"], \
                dbname = cliArguments["dbname"], dbusername = cliArguments["dbusername"], \
                dbpassword = cliArguments["dbuserpassword"], trustedmode = cliArguments["trustedmode"], \
                viewname = cliArguments["viewname"])


            connector.Open()
            surveyStructureDF:pd.DataFrame = getSurveyStructure(connector)

            if(doesPersistenceFileExist(cliArguments["persistencefilepath"]) == False):

                if(isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]) == True):
                    
                    
                    #pickle the dataframe in the path given by persistencefilepath
                    #TODO

                    df_savedSurveyStructure = surveyStructureDF.drop(surveyStructureDF.index[3])
                    df_savedSurveyStructure.to_csv(cliArguments['persistencefilepath'], index=False, header=True)

                    print("\nINFO - Content of SurveyResults table pickled in " + cliArguments["persistencefilepath"] + "\n")
                    
                    #refresh the view using the function written for this purpose
                    #TODO
                    refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments['viewname'])
            else:
                #Compare the existing pickled SurveyStructure file with surveyStructureDF
                # What do you need to do if the dataframe and the pickled file are different?
                #TODO
                #pass #pass only written here for not creating a syntax error, to be removed
            
            #get your survey results from the view in a dataframe and save it to a CSV file in the path given by resultsfilepath
            #TODO

                if compareDBSurveyStructureToPersistenceFile(surveyStructureDF, cliArguments["persistencefilepath"]) :
                    print("New SurveyStructure is same as saved one, do nothing")
                else:
                     print('SurveyStructure is different than saved one, need to trigger view')
                     surveyStructureDF.to_csv(cliArguments['persistencefilepath'], index=False, header=True)
                     print("\nINFO - Content of SurveyResults table pickled in " + cliArguments['persistencefilepath'] )   
                     refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments['viewname'])

                surveyResultsDF = surveyResultsToDF(connector,cliArguments['viewname'])
                surveyResultsDF.to_csv(cliArguments["resultsfilepath"], index=False, header=True)

                print("\nDONE - Results exported in " + cliArguments["resultsfilepath"] + "\n")

            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return



if __name__ == '__main__':
    main()