from django.shortcuts import render, redirect
from django.http import HttpResponse

from forms import UploadFileForm
from models import FileUpload

import os

import zipfile

import numpy as np
import pandas as pd
from pandas import Series, DataFrame


def index_view(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES) #form
        if form.is_valid():
            #save the file into the database 
            fileupload = FileUpload(filename = request.FILES['filename'])
            fileupload.save()
            # html = '<html><body>The form was valid. Form was %s. Filename was %s (model). finally, in the post request. Request.FILES[\'filename\'] was %s </body></html>' % (form, filename, request.FILES['filename'])
            return redirect('/handlefile')
        return HttpResponse("<html><body>Form was not valid</body></html>")
    else:
        form = UploadFileForm() #empty, unbound form
        files = FileUpload.objects.all()
        return render(request, 'index.html', {'form': form})
        
def handle_file(request):
    length = len(FileUpload.objects.all())
    file = FileUpload.objects.all()[length - 1] #get the latest file (again, the actual filename, and the email)
    
    #Series of Checks

    #First Check if the file is a zip file
    if not zipfile.is_zipfile(file.filename):
    	return HttpResponse("<html><body>The file you provided is not a zip file. Please try again.</body></html>")

    #Second check to make sure the csv files are in place
    #and extract files
    CSVFiles = ['Export.csv', 'Project.csv', 'ProjectCoC.csv', 'Funder.csv', 'Client.csv',
            'Enrollment.csv', 'EnrollmentCoC.csv',
            'Exit.csv', 'IncomeBenefits.csv', 'Disabilities.csv', 'HealthAndDV.csv', 
            'EmploymentEducation.csv', 'Services.csv']   

    testzip = zipfile.ZipFile(file.filename, 'r')
    filedir = 'upload' + str(length - 1) + '/' #Create a directory name (upload + number)

    namelist = testzip.namelist()

    revlist = []
    missing = []
    for nm in CSVFiles:
    	if nm in namelist and nm not in revlist:
    		testzip.extract(nm, filedir)
    		revlist.append(nm)
    	else:
    		missing.append(nm)
    if len(missing) > 0:
    	html = "<html><body>The following files were missing from the ZIP file: %s </body></html>" % missing
    	return HttpResponse(html)


    #csv converted into dataframes will be saved in export:
    # 0: export
    # 1: project
    # 2: projectcoc
    # 3: funder
    # 4: client
    # 5: enrollment
    # 6: enrollmentcoc
    # 7: exit
    # 8: incomebenefits
    # 9: disabilities
    # 10: healthanddv
    # 11: employmenteducation
    # 12: services

    export = []

    for n in range(len(CSVFiles)):
    	exportfile = filedir + CSVFiles[n]
    	export.append(pd.read_csv(exportfile))

    #Do the tricks!

    projectfile = export[1] #Read Project.csv as a DF
    projectcocfile = export[2]
    funderfile = export[3]
    clientfile = export[4]
    enrollmentfile = export[5]
    enrollmentcocfile = export[6]
    exitfile = export[7]
    incomebenefitsfile = export[8]
    disabilitiesfile = export[9]
    healthanddvfile = export[10]
    employmenteducationfile = export[11]
    servicesfile = export[12]

    #It all starts with unique IDs in Enrollment.csv
    collapsedfile = enrollmentfile

    #1) link: PersonalID in Enrollment.csv to PersonalID in client.csv
    collapsedfile = pd.merge(collapsedfile, clientfile, how='left', on='PersonalID', copy=False)

    #2) link: ProjectEntryID in collapsedfile to exit.csv
    collapsedfile = pd.merge(collapsedfile, exitfile, how='left', on='ProjectEntryID', copy=False)

    #3) before linking disabilities file, we need to collapse it.
    # we will create a column for each (disability type + datacollectionstage).
    # There should be 6*4 = 24 columns, but that will differ 

    #Get the values for DisabilityType and DataCollectionStage in the disabilities files
    DisabilityTypes = disabilitiesfile['DisabilityType'].unique()
    DataCollectionStages = disabilitiesfile['DataCollectionStage'].unique()

    #Subtract 'Annual Assessment' from DataCollectionStage (it will mixed in with update)
    DataCollectionStages = np.setdiff1d(DataCollectionStages, np.array([5]))
    #Subtract 'HIV/AIDS' from DisabilityType (this info was not required)
    DisabilityTypes = np.setdiff1d(DisabilityTypes, np.array([8]))

    disSlice = collapsedfile[['ProjectEntryID', 'PersonalID']]

    for d in DisabilityTypes:
    	for c in DataCollectionStages:
    		if c == 2:
    			c = [2, 5] 
    		else:
    			c = [c]
    		disSlice1 = disabilitiesfile[(disabilitiesfile['DisabilityType'] == d) & (disabilitiesfile['DataCollectionStage'].isin(c))]
    		disSlice1.sort_values(['InformationDate'], ascending=False, inplace=True)
    		disSlice1.drop_duplicates(subset='ProjectEntryID', inplace=True)

    		if d == 5:
    			columnleft = 'PhysicalDisability'
    		elif d == 6:
    			columnleft = 'DevelopmentalDisability'
    		elif d == 7:
    			columnleft = 'ChronicHealthCondition'
    		elif d == 8:
    			continue
    		elif d == 9:
    			columnleft = 'MentalHealthProblem'
    		elif d == 10:
    			columnleft = 'SubstanceAbuse'

    		if c == [1]:
    			columnright = 'AtEntry'
    		elif c == [2, 5]:
    			columnright = 'AtLastUpdate'
    		elif c == [3]:
    			columnright = 'AtExit'

    		columnname = columnleft + columnright
    		columnnameRecTx = columnname + 'ReceivingServices'

    		disSlice1[columnname] = 1
    		disSlice1[columnnameRecTx] = disSlice1['ReceivingServices']
    		disSlice1 = disSlice1[['ProjectEntryID', columnname, columnnameRecTx]]
    		disSlice = pd.merge(disSlice, disSlice1, how='left', on='ProjectEntryID', copy=False)
    		disSlice.replace(np.nan, 0, inplace=True)

    collapsedfile = pd.merge(collapsedfile, disSlice, how='left', on='ProjectEntryID')


    #Reorder all columns
    #collapsedfile = collapsedfile[['ProjectEntryID', 'PersonalID', 'ProjectID', 'HouseholdID', 'RelationshipToHoH' 'FirstName', 'MiddleName', 'LastName', 'NameSuffix', 'NameDataQuality', 
    #								'SSN', 'SSNDataQuality', 'DOB', 'DOBDataQuality', 'AmIndAKNative', 'Asian', 'BlackAfAmerican', 'NativeHIOtherPacific',
    #								'White', 'RaceNone', 'Ethnicity', 'Gender', 'SexualOrientation', 'FormerWardChildWelfare', 'ChildWelfareYears', 'ChildWelfareMonths', 'FormerWardJuvenileJustice',
    #								'JuvenileJusticeYears', 'JuvenileJusticeMonths', 'VeteranStatus',  'EntryDate', 'ExitDate', 'Destination', 'OtherDestination', 'WrittenAftercarePlan', 'AssistanceMainstreamBenefits',
    #								'PermanentHousingPlacement', 'TemporaryShelterPlacement', 'ExitCounseling', 'FurtherFollowUpServices', 'ScheduledFollowUpContacts',
    #								'ResourcePackage', 'OtherAftercarePlanOrAction', 'ProjectCompletionStatus', 'EarlyExitReason', 'FamilyReunificationAchieved', 'ResidencePrior', 'ResidencePriorLengthOfStay',
    #								'LOSUnderThreshold', 'PreviousStreetESSH', 'DateToStreetESSH', 'TimesHomelessPastThreeYears', 'MonthsHomelessPastThreeYears',
    #								'DisablingCondition', #Instert Number of Contacts Here and Contact Date
    #								'DateOfEngagement', 'DateOfBCPStatus', 'FYSBYouth', 'ReasonNoServices', 'HouseholdDynamics', 'SexualOrientationGenderIDYouth',
    #								'SexualOrientationGenderIDFam', 'HousingIssuesYouth', 'HousingIssuesFam', 'SchoolEducationalIssuesYouth', 'SchoolEducationIssuesFam',
    #								'UnemploymentYouth', 'UnemploymentFam', 'MentalHealthIssuesYouth', 'MentalHealthIssuesFam', 'HealthIssuesYouth', 'HealthIssuesFam',
    #								'PhysicalDisabilityYouth', 'PhysicalDisabilityFam', 'MentalDisabilityYouth', 'MentalDisabilityFam', 'AbuseAndNeglectYouth', 'AbuseAndNeglectFam',
    #								'AlcoholDrugAbuseYouth', 'AlcoholDrugAbuseFam', 'InsufficientIncome', 'ActiveMilitaryParent', 'IncarceratedParent', 'IncarceratedParentStatus',
    #								'ReferralSource', 'CountOutreachReferralApproaches', 'ExchangeForSex', 'ExchangeForSexPastThreeMonths', 'CountOfExchangeForSex', 'AskedOrForcedToExchangeForSex',
    #								'AskedOrForcedToExchangeForSexPastThreeMonths', 'WorkPlaceViolenceThreats', 'WorkplacePromiseDifference' 'CoercedToContinueWork', 'LaborExploitPastThreeMonths']]



    collapsedfile.to_csv('CollapsedDIS1.csv', index=False, header=True, sep=',')


    return HttpResponse(collapsedfile)








    project1 = projectfile[projectfile['ProjectType'].isin([1, 4, 8])] #Select only SO, ES, or SH projects
    project2 = projectfile[projectfile['ProjectType'].isin([2, 3, 6, 9, 10, 11, 12, 13, 14])] #3.197B institutions
    projectid1 = project1['ProjectID'].tolist() #Select projectids for 3.917A lit homeless
    projectid2 = project2['ProjectID'].tolist() #Select projectids for 3.917B institutions

    enrollmentfile = export[5]
  
    #For both A and B, change ResidencePrior = 17--> 99
    enrollmentfile['ResidencePrior'].replace(17, 99, inplace=True)
    
    #LOSUnderThrehold for enrollment1 should be 99
    enrollmentfile['LOSUnderThreshold'] = 99

    #Set all institutions under 90 days 
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([4, 5, 6, 7, 15, 24])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([2, 3, 10, 11])), 'LOSUnderThreshold'] = 1 
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([14, 23, 21, 3, 22, 19, 25, 20, 26, 12, 13, 2, 17, 8, 9, 99])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([10, 11])), 'LOSUnderThreshold'] = 1
    
    #Over 90 days
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([4, 5, 6, 7, 15, 24])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([4, 5, 8, 9, 99])), 'LOSUnderThreshold'] = 0
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([14, 23, 21, 3, 22, 19, 25, 20, 26, 12, 13, 2, 17, 8, 9, 99])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([2, 3, 4, 5, 8, 9, 99])), 'LOSUnderThreshold'] = 0

    #enrollmentfile[(enrollmentfile['ProjectID'].isin(projectid2)) & ~(enrollmentfile['ResidencePrior'].isin([4, 5, 6, 7, 15, 24, 14, 23, 21, 3, 22, 19, 25, 20, 26, 12, 13, 2, 17, 8, 9, 99])), 'LOSUnderThreshold'] = 99
    
    #PreviousStreetESSH
    enrollmentfile['PreviousStreetESSH'] = 888
    
    #Institutional
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([15, 6, 7, 24, 4, 5])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([10, 11, 2, 3])), 'PreviousStreetESSH'] = enrollmentfile['EntryFromStreetESSH']
    
    #TH/PH
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([14, 23, 21, 3, 22, 19, 25, 20, 26, 12, 13, 2, 8, 9, 99])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([10, 11])), 'PreviousStreetESSH'] = enrollmentfile['EntryFromStreetESSH']
    
    #From Homeless
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([16, 1, 18])), 'PreviousStreetESSH'] = 888
    
    enrollmentfile['PreviousStreetESSH'].replace(8, 0, inplace=True)
    enrollmentfile['PreviousStreetESSH'].replace(9, 0, inplace=True)
    enrollmentfile['PreviousStreetESSH'].replace(99, 0, inplace=True)
    enrollmentfile['PreviousStreetESSH'].replace(888, 99, inplace=True)
    
    enrollmentfile['NewDateToStreetESSH'] = ''
    
    #Map All Dates into NewDateToStreetESSH from DateToStreetESSH for 3.197A
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid1)), 'NewDateToStreetESSH'] = enrollmentfile['DateToStreetESSH']

    #3.197B
    #Homeless
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([16, 1, 18])), 'NewDateToStreetESSH'] = enrollmentfile['DateToStreetESSH']
    
    #Institution
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([15,  6, 7, 24, 4, 5])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([10, 11, 2, 3])) & (enrollmentfile['PreviousStreetESSH']==1), 'NewDateToStreetESSH'] = enrollmentfile['DateToStreetESSH']

    #Others
    enrollmentfile.ix[(enrollmentfile['ProjectID'].isin(projectid2)) & (enrollmentfile['ResidencePrior'].isin([14, 23, 21, 3, 22, 19, 25, 20, 26, 12, 13, 2, 17, 8, 9, 99])) & (enrollmentfile['ResidencePriorLengthOfStay'].isin([10, 11])) & (enrollmentfile['PreviousStreetESSH']==1), 'NewDateToStreetESSH'] = enrollmentfile['DateToStreetESSH']


    #Replace NewStreetDateToStreetESSH with DateToStreetESSH
    del enrollmentfile['DateToStreetESSH']
    enrollmentfile.rename(index=str, columns={"NewDateToStreetESSH": "DateToStreetESSH"}, inplace=True)
    
    #TimesHomelessPastThreeYears
    enrollmentfile['TimesHomelessPastThreeYears'].replace(0, 1, inplace=True)
    enrollmentfile.ix[(enrollmentfile['DateToStreetESSH']==''), 'TimesHomelessPastThreeYears'] = 99
    
    enrollmentfile.ix[(enrollmentfile['TimesHomelessPastThreeYears']==99), 'MonthsHomelessPastThreeYears'] = 99

    #DisablingCondition in enrollment.csv -- change any blanks to 99
    enrollmentfile['DisablingCondition'].replace('', 99, inplace=True)
    enrollmentfile['DisablingCondition'].replace(np.nan, 99, inplace=True)

    #Add HouseholdID in enrollmentcoc.csv -- 
    # For enrollment.csv, create a dictionary {PersonalID: HouseholdID}

    PersonToHouse = dict()
    #row_iterator = enrollmentfile.iterrows()

    for i, row_i in enrollmentfile.iterrows():
        PersonalID = row_i['ProjectEntryID']
        HouseholdID = row_i['HouseholdID']
        PersonToHouse[PersonalID] = HouseholdID

    #Add HP Targeting Critieria and Use of Other Crisis Criteria in enrollment.csv
    enrollmentfile['UrgentReferral'] = ''
    enrollmentfile['TimeToHousingLoss'] = ''
    enrollmentfile['ZeroIncome'] ='' 
    enrollmentfile['AnnualPercentAMI'] = ''
    enrollmentfile['FinancialChange'] = '' 
    enrollmentfile['HouseholdChange'] = ''
    enrollmentfile['EvictionHistory'] = ''
    enrollmentfile['SubsidyAtRisk'] = ''
    enrollmentfile['LiteralHomelessHistory'] = ''
    enrollmentfile['DisabledHoH'] = ''
    enrollmentfile['CriminalRecord'] = ''
    enrollmentfile['SexOffender'] = ''
    enrollmentfile['DependentUnder6'] = '' 
    enrollmentfile['SingleParent'] = ''
    enrollmentfile['HH5Plus'] = ''  
    enrollmentfile['IraqAfghanistan'] = ''
    enrollmentfile['FemVet'] = ''
    enrollmentfile['ThresholdScore'] = ''
    enrollmentfile['ERVisits'] = ''
    enrollmentfile['JailNights'] = ''
    enrollmentfile['HospitalNights'] = ''
    
    #convert floats to int
    for n in (enrollmentfile.select_dtypes(include=[float])):
        if enrollmentfile[n].notnull().sum() > 0: 
            if 'ChildWelfareMonths' in n or 'JuvenileJusticeMonths' in n or 'CountOutreachReferralApproaches' in n:
                enrollmentfile[n].replace(np.nan, 0, inplace=True)
                enrollmentfile[n] = enrollmentfile[n].astype(int)
            elif 'Date' not in n and 'Percent' not in n and 'ID' not in n and 'LastPermanent' not in n and 'VAMCStation' not in n:
                enrollmentfile[n].replace(np.nan, 99, inplace=True)
                enrollmentfile[n] = enrollmentfile[n].astype(int)
                
    #Better idea -- convert I columns with nan values to object values
    #integerList = []
    for n in integerList:
        if enrollmentfile[n].notnull().sum() > 0: 
            as_object = enrollmentfile['ResidencePrior'].fillna(0).astype(np.int64).astype(np.object)
            as_object[enrollmentfile['ResidencePrior'].isnull()]=""

    #enrollmentfile.to_csv('test.csv', index=False, float_format='%.2f') #this works


    # Input the values based on the personalid as key in enrollmentcoc.csv
    enrollmentcocfile = export[6]
    enrollmentcocfile['HouseholdID'] = enrollmentcocfile['ProjectEntryID']
    enrollmentcocfile['HouseholdID'].map(PersonToHouse)
    enrollmentcocfile = enrollmentcocfile[['EnrollmentCoCID', 'ProjectEntryID', 'HouseholdID', 'ProjectID', 'PersonalID', 'InformationDate', 'CoCCode', 'DataCollectionStage',
                    'DateCreated', 'DateUpdated', 'UserID', 'DateDeleted', 'ExportID']]
                    
    #delete the abrogated columns in enrollment.csv
    del enrollmentfile['OtherResidencePrior']
    del enrollmentfile['EntryFromStreetESSH']
    del enrollmentfile['InPermanentHousing']

    #change the order of columns in enrollment.csv
    enrollmentfile = enrollmentfile[['ProjectEntryID', 'PersonalID', 'ProjectID', 'EntryDate', 'HouseholdID', 'RelationshipToHoH',
                    'ResidencePrior', 'ResidencePriorLengthOfStay', 'LOSUnderThreshold', 'PreviousStreetESSH', 'DateToStreetESSH',
                    'TimesHomelessPastThreeYears', 'MonthsHomelessPastThreeYears', 'DisablingCondition',
                    'HousingStatus', 'DateOfEngagement', 'ResidentialMoveInDate', 'DateOfPATHStatus',
                    'ClientEnrolledInPATH', 'ReasonNotEnrolled', 'WorstHousingSituation', 'PercentAMI', 'LastPermanentStreet',
                    'LastPermanentCity', 'LastPermanentState', 'LastPermanentZIP', 'AddressDataQuality', 'DateOfBCPStatus',
                    'FYSBYouth', 'ReasonNoServices', 'SexualOrientation', 'FormerWardChildWelfare', 'ChildWelfareYears',
                    'ChildWelfareMonths', 'FormerWardJuvenileJustice', 'JuvenileJusticeYears', 'JuvenileJusticeMonths', 'HouseholdDynamics',
                    'SexualOrientationGenderIDYouth', 'SexualOrientationGenderIDFam', 'HousingIssuesYouth', 'HousingIssuesFam',
                    'SchoolEducationalIssuesYouth', 'SchoolEducationalIssuesFam', 'UnemploymentYouth', 'UnemploymentFam',
                    'MentalHealthIssuesYouth', 'MentalHealthIssuesFam', 'HealthIssuesYouth', 'HealthIssuesFam', 'PhysicalDisabilityYouth',
                    'PhysicalDisabilityFam', 'MentalDisabilityYouth', 'MentalDisabilityFam', 'AbuseAndNeglectYouth',
                    'AbuseAndNeglectFam', 'AlcoholDrugAbuseYouth', 'AlcoholDrugAbuseFam', 'InsufficientIncome', 'ActiveMilitaryParent',
                    'IncarceratedParent', 'IncarceratedParentStatus', 'ReferralSource', 'CountOutreachReferralApproaches', 'ExchangeForSex',
                    'ExchangeForSexPastThreeMonths', 'CountOfExchangeForSex', 'AskedOrForcedToExchangeForSex', 'AskedOrForcedToExchangeForSexPastThreeMonths',
                    'WorkPlaceViolenceThreats', 'WorkplacePromiseDifference', 'CoercedToContinueWork', 'LaborExploitPastThreeMonths',
                    'UrgentReferral', 'TimeToHousingLoss', 'ZeroIncome', 'AnnualPercentAMI', 'FinancialChange', 'HouseholdChange', 'EvictionHistory',
                    'SubsidyAtRisk', 'LiteralHomelessHistory', 'DisabledHoH', 'CriminalRecord', 'SexOffender', 'DependentUnder6',
                    'SingleParent', 'HH5Plus', 'IraqAfghanistan', 'FemVet', 'HPScreeningScore', 'ThresholdScore',
                    'VAMCStation', 'ERVisits', 'JailNights', 'HospitalNights', 'DateCreated', 'DateUpdated', 'UserID', 'DateDeleted', 'ExportID']]

    #Additional changes in other files
    #Add SourceType in export.csv and change all values to 4 
    exportfile = export[0]
    exportfile['SourceType'] = 4
    exportfile = exportfile[['ExportID', 'SourceType', 'SourceID', 'SourceName', 'SourceContactFirst', 'SourceContactLast',
                'SourceContactPhone', 'SourceContactExtension', 'SourceContactEmail', 'ExportDate',
                'ExportStartDate', 'ExportEndDate', 'SoftwareName', 'SoftwareVersion', 'ExportPeriodType',
                'ExportDirective', 'HashStatus']]

    #Remove OtherGender field from client.csv
    clientfile = export[4]
    del clientfile['OtherGender']

    #Add IndianHealthServices and NoIndianHealthServicesReason in IncomeBenefits.csv
    incomebenefitsfile = export[8]
    incomebenefitsfile['IndianHealthServices'] = 99
    incomebenefitsfile['NoIndianHealthServicesReason'] = 99
    incomebenefitsfile['OtherInsurance'] = 99
    incomebenefitsfile['OtherInsuranceIdentify'] = 99
    
    for n in (incomebenefitsfile.select_dtypes(include=[float])):
        if incomebenefitsfile[n].notnull().sum() > 0: 
            if 'Date' not in n and 'ID' not in n and 'TotalMonthlyIncome' not in n and 'Amount' not in n and 'OtherIncomeSourceIdentify' not in n and 'OtherBenefitsSourceIdentify' not in n and 'DataCollectionStage' not in n:
                incomebenefitsfile[n].replace(np.nan, 99, inplace=True)
                incomebenefitsfile[n] = incomebenefitsfile[n].astype(int)
    
    incomebenefitsfile = incomebenefitsfile[['IncomeBenefitsID', 'ProjectEntryID', 'PersonalID', 'InformationDate', 'IncomeFromAnySource', 'TotalMonthlyIncome', 'Earned',
                    'EarnedAmount', 'Unemployment', 'UnemploymentAmount', 'SSI', 'SSIAmount', 'SSDI', 'SSDIAmount', 'VADisabilityService',
                    'VADisabilityServiceAmount', 'VADisabilityNonService', 'VADisabilityNonServiceAmount', 'PrivateDisability', 'PrivateDisabilityAmount',
                    'WorkersComp', 'WorkersCompAmount', 'TANF', 'TANFAmount', 'GA', 'GAAmount', 'SocSecRetirement', 'SocSecRetirementAmount', 'Pension', 'PensionAmount',
                    'ChildSupport', 'ChildSupportAmount', 'Alimony', 'AlimonyAmount', 'OtherIncomeSource', 'OtherIncomeAmount', 'OtherIncomeSourceIdentify', 'BenefitsFromAnySource',
                    'SNAP', 'WIC', 'TANFChildCare', 'TANFTransportation', 'OtherTANF', 'RentalAssistanceOngoing', 'RentalAssistanceTemp', 'OtherBenefitsSource', 'OtherBenefitsSourceIdentify',
                    'InsuranceFromAnySource', 'Medicaid', 'NoMedicaidReason', 'Medicare', 'NoMedicareReason', 'SCHIP', 'NoSCHIPReason', 'VAMedicalServices',
                    'NoVAMedReason', 'EmployerProvided', 'NoEmployerProvidedReason', 'COBRA', 'NoCOBRAReason', 'PrivatePay', 'NoPrivatePayReason', 'StateHealthIns',
                    'NoStateHealthInsReason', 'IndianHealthServices', 'NoIndianHealthServicesReason', 'OtherInsurance', 'OtherInsuranceIdentify', 'HIVAIDSAssistance', 'NoHIVAIDSAssistanceReason', 'ADAP', 'NoADAPReason', 'DataCollectionStage', 'DateCreated', 'DateUpdated',
                    'UserID', 'DateDeleted', 'ExportID']]

    # save export.csv, enrollment.csv, enrollmentcoc.csv, incomebenefits.csv as csv
    newpath = filedir + 'newzip'
    if not os.path.exists(newpath):
        os.makedirs (filedir + 'newzip')
    exportfile.to_csv(filedir + 'newzip/Export.csv', sep=',', header=True, index=False)
    enrollmentfile.to_csv(filedir + 'newzip/Enrollment.csv', sep=',', header=True, index=False)
    enrollmentcocfile.to_csv(filedir + 'newzip/EnrollmentCoC.csv', sep=',', header=True, index=False)
    incomebenefitsfile.to_csv(filedir + 'newzip/IncomeBenefits.csv', sep=',', header=True, index=False)
    clientfile.to_csv(filedir + 'newzip/Client.csv', sep=',', header=True, index=False)

    #untouched files
    export[1].to_csv(filedir + 'newzip/Project.csv', sep=',', header=True, index=False)
    export[2].to_csv(filedir + 'newzip/ProjectCoC.csv', sep=',', header=True, index=False)
    export[3].to_csv(filedir + 'newzip/Funder.csv', sep=',', header=True, index=False)
    export[7].to_csv(filedir + 'newzip/Exit.csv', sep=',', header=True, index=False)
    export[9].to_csv(filedir + 'newzip/Disabilities.csv', sep=',', header=True, index=False)
    export[10].to_csv(filedir + 'newzip/HealthAndDV.csv', sep=',', header=True, index=False)
    export[11].to_csv(filedir + 'newzip/EmploymentEducation.csv', sep=',', header=True, index=False)
    export[12].to_csv(filedir + 'newzip/Services.csv', sep=',', header=True, index=False)

    # zip all into 
    # 0: export
    # 1: project
    # 2: projectcoc
    # 3: funder
    # 4: client
    # 5: enrollment
    # 6: enrollmentcoc
    # 7: exit
    # 8: incomebenefits
    # 9: disabilities
    # 10: healthanddv
    # 11: employmenteducation

    # 12: services

    with zipfile.ZipFile(filedir + 'Exportto50.zip', 'w') as myzip:
        myzip.write(filedir + 'newzip/Export.csv')
        myzip.write(filedir + 'newzip/Project.csv')
        myzip.write(filedir + 'newzip/ProjectCoC.csv')
        myzip.write(filedir + 'newzip/Funder.csv')
        myzip.write(filedir + 'newzip/Client.csv')
        myzip.write(filedir + 'newzip/Enrollment.csv')
        myzip.write(filedir + 'newzip/EnrollmentCoC.csv')
        myzip.write(filedir + 'newzip/Exit.csv')
        myzip.write(filedir + 'newzip/IncomeBenefits.csv')
        myzip.write(filedir + 'newzip/Disabilities.csv')
        myzip.write(filedir + 'newzip/HealthAndDV.csv')
        myzip.write(filedir + 'newzip/EmploymentEducation.csv')
        myzip.write(filedir + 'newzip/Services.csv')


    return HttpResponse('<html><body><h1>Congratulations! Your file was converted successfully. </h1></body></html>')
