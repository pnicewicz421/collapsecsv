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

    #New idea. We will only have one disability type and receivingservices at entry, at update (including ann. assessment), and at exit
    #Constacts for Data Collection Stages

    #Sort by latest date first, as this will take the latest update when removing duplicates
    disabilitiesfile.sort_values(['InformationDate'], ascending=False, inplace=True)

    for d in DisabilityTypes:
                
        if d == 5:
    		columnleft = 'PhysicalDisability'
    	elif d == 6:
    		columnleft = 'DevelopmentalDisability'
    	elif d == 7:
    		columnleft = 'ChronicHealthCondition'
    	elif d == 9:
    		columnleft = 'MentalHealthProblem'
    	elif d == 10:
    		columnleft = 'SubstanceAbuse'

        disabilitiesfile[columnleft] = 0
        disabilitiesfile.ix[(disabilitiesfile['DisabilityType'] == d), columnleft] = 1
     
        # Create a ReceivingServices column for entry, update, and exit
        for c in DataCollectionStages:
            if c == 2:
                col = [2, 5] 
                Column = 'ReceivingServicesFor' + columnleft + 'AtUpdate'
            elif c == 1:
                col = [1]
                Column = 'ReceivingServicesFor' + columnleft + 'AtEntry'
            elif c == 3:
                col = [3]
                Column = 'ReceivingServicesFor' + columnleft + 'AtExit'

            disabilitiesfile[Column] = 0

            Value = disabilitiesfile.ix[((disabilitiesfile['DisabilityType'] == d) & (disabilitiesfile['DataCollectionStage'].isin(col))), ['ProjectEntryID', 'ReceivingServices']]
            Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
            Value.set_index(['ProjectEntryID'], inplace=True)
            ValueDict = Value.to_dict('dict').values()[0]
            disabilitiesfile[Column] = disabilitiesfile['ProjectEntryID'].map(ValueDict)

    collapsedfile = pd.merge(collapsedfile, disabilitiesfile, how='left', on='ProjectEntryID', copy=False)

    #EmploymentEducation --> create Last Grade Completed, School Status, Employment Status, Employment Type, Not Employed Type
    #                        by Data Collection Stage
    DataCollectionStages = employmenteducationfile['DataCollectionStage'].unique()
    DataCollectionStages = np.setdiff1d(DataCollectionStages, np.array([2, 5]))

    employmenteducationfile.sort_values(['InformationDate'], ascending=False, inplace=True)

    LastGradeCompletedColumn = 'LastGradeCompleted'
    SchoolStatusColumn = 'SchoolStatus'
    EmployedColumn = 'Employed'
    EmploymentTypeColumn = 'EmploymentType'
    NotEmployedReasonColumn = 'NotEmployedReason'

    for c in DataCollectionStages:
        if c == 1:
            col = [1]
            Column = 'AtEntry'
        elif c == 3:
            col = [3]
            Column = 'AtExit'

        employmenteducationfile[LastGradeCompletedColumn + Column] = 0
        employmenteducationfile[SchoolStatusColumn + Column] = 0
        employmenteducationfile[EmployedColumn + Column] = 0
        employmenteducationfile[EmploymentTypeColumn + Column] = 0
        employmenteducationfile[NotEmployedReasonColumn + Column] = 0

        LastGradeValue = employmenteducationfile.ix[(employmenteducationfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', LastGradeCompletedColumn]]
        LastGradeValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        LastGradeValue.set_index(['ProjectEntryID'], inplace=True)
        LastGradeValueDict = Value.to_dict('dict').values()[0]

        SchoolStatusValue = employmenteducationfile.ix[(employmenteducationfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', SchoolStatusColumn]]
        SchoolStatusValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        SchoolStatusValue.set_index(['ProjectEntryID'], inplace=True)
        SchoolStatusValueDict = Value.to_dict('dict').values()[0]

        EmployedValue = employmenteducationfile.ix[(employmenteducationfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', EmployedColumn]]
        EmployedValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        EmployedValue.set_index(['ProjectEntryID'], inplace=True)
        EmployedValueDict = Value.to_dict('dict').values()[0]
        
        EmploymentTypeValue = employmenteducationfile.ix[(employmenteducationfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', EmploymentTypeColumn]]
        EmploymentTypeValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        EmploymentTypeValue.set_index(['ProjectEntryID'], inplace=True)
        EmploymentTypeValueDict = Value.to_dict('dict').values()[0]

        NotEmployedReasonValue = employmenteducationfile.ix[(employmenteducationfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', NotEmployedReasonColumn]]
        NotEmployedReasonValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        NotEmployedReasonValue.set_index(['ProjectEntryID'], inplace=True)
        NotEmployedReasonValueDict = Value.to_dict('dict').values()[0]
                   
        employmenteducationfile[LastGradeCompletedColumn + Column] = employmenteducationfile['ProjectEntryID'].map(LastGradeValueDict)
        employmenteducationfile[SchoolStatusColumn + Column] = employmenteducationfile['ProjectEntryID'].map(SchoolStatusValueDict)
        employmenteducationfile[EmployedColumn + Column] = employmenteducationfile['ProjectEntryID'].map(EmployedValueDict)
        employmenteducationfile[EmploymentTypeColumn + Column] = employmenteducationfile['ProjectEntryID'].map(EmploymentTypeValueDict)
        employmenteducationfile[NotEmployedReasonColumn + Column] = employmenteducationfile['ProjectEntryID'].map(NotEmployedReasonValueDict)

    collapsedfile = pd.merge(collapsedfile, employmenteducationfile, how='left', on='ProjectEntryID', copy=False)
    
    #HealthAndDV - same as with EmploymentEducation but for the following columns:
    # GeneralHealthStatus    DentalHealthStatus  MentalHealthStatus  PregnancyStatus DueDate by DataCollectionStage
    DataCollectionStages = healthanddvfile['DataCollectionStage'].unique()
    DataCollectionStages = np.setdiff1d(DataCollectionStages, np.array([5]))

    employmenteducationfile.sort_values(['InformationDate'], ascending=False, inplace=True)

    GeneralHealthStatusColumn = 'GeneralHealthStatus'
    DentalHealthStatusColumn = 'DentalHealthStatus'
    MentalHealthStatusColumn = 'MentalHealthStatus'
    PregnancyStatusColumn = 'PregnancyStatus'
    DueDateColumn = 'DueDate'

    for c in DataCollectionStages:
        if c == 2:
            col = [2, 5] 
            Column = 'AtUpdate'
        elif c == 1:
            col = [1]
            Column = 'AtEntry'
        elif c == 3:
            col = [3]
            Column = 'AtExit'

        healthanddvfile[GeneralHealthStatusColumn + Column] = 0
        healthanddvfile[DentalHealthStatusColumn + Column] = 0
        healthanddvfile[MentalHealthStatusColumn + Column] = 0
        healthanddvfile[PregnancyStatusColumn + Column] = 0
        healthanddvfile[DueDateColumn + Column] = 0

        GeneralHealthStatusValue = healthanddvfile.ix[(healthanddvfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', GeneralHealthStatusColumn]]
        GeneralHealthStatusValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        GeneralHealthStatusValue.set_index(['ProjectEntryID'], inplace=True)
        GeneralHealthStatusValueDict = Value.to_dict('dict').values()[0]

        DentalHealthStatusValue = healthanddvfile.ix[(healthanddvfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', DentalHealthStatusColumn]]
        DentalHealthStatusValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        DentalHealthStatusValue.set_index(['ProjectEntryID'], inplace=True)
        DentalHealthStatusValueDict = Value.to_dict('dict').values()[0]

        MentalHealthStatusValue = healthanddvfile.ix[(healthanddvfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', MentalHealthStatusColumn]]
        MentalHealthStatusValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        MentalHealthStatusValue.set_index(['ProjectEntryID'], inplace=True)
        MentalHealthStatusValueDict = Value.to_dict('dict').values()[0]
        
        PregnancyStatusValue = healthanddvfile.ix[(healthanddvfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', PregnancyStatusColumn]]
        PregnancyStatusValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        PregnancyStatusValue.set_index(['ProjectEntryID'], inplace=True)
        PregnancyStatusValueDict = Value.to_dict('dict').values()[0]

        DueDateValue = healthanddvfile.ix[(healthanddvfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', DueDateColumn]]
        DueDateValue.drop_duplicates(subset='ProjectEntryID', inplace=True)
        DueDateValue.set_index(['ProjectEntryID'], inplace=True)
        DueDateValueDict = Value.to_dict('dict').values()[0]
                   
        healthanddvfile[GeneralHealthStatusColumn + Column] = healthanddvfile['ProjectEntryID'].map(GeneralHealthStatusValueDict)
        healthanddvfile[DentalHealthStatusColumn + Column] = healthanddvfile['ProjectEntryID'].map(DentalHealthStatusValueDict)
        healthanddvfile[MentalHealthStatusColumn + Column] = healthanddvfile['ProjectEntryID'].map(MentalHealthStatusValueDict)
        healthanddvfile[PregnancyStatusColumn + Column] = healthanddvfile['ProjectEntryID'].map(PregnancyStatusValueDict)
        healthanddvfile[DueDateColumn + Column] = healthanddvfile['ProjectEntryID'].map(DueDateValueDict)

    collapsedfile = pd.merge(collapsedfile, healthanddvfile, how='left', on='ProjectEntryID', copy=False)

    #IncomeBenefits
    #IncomeFromAnySource    TotalMonthlyIncome  Earned  EarnedAmount   
    #Unemployment    UnemploymentAmount  SSI SSIAmount   SSDI    SSDIAmount  
    #VADisabilityService VADisabilityServiceAmount   VADisabilityNonService  VADisabilityNonServiceAmount    
    #PrivateDisability   PrivateDisabilityAmount WorkersComp WorkersCompAmount   TANF    TANFAmount  GA  GAAmount   
    # SocSecRetirement    SocSecRetirementAmount  ChildSupport    ChildSupportAmount  Alimony AlimonyAmount 
    #  OtherIncomeSource   OtherIncomeAmount   OtherIncomeSourceIdentify   BenefitsFromAnySource   SNAP  
    #  WIC TANFChildCare   TANFTransportation  OtherTANF   RentalAssistanceOngoing RentalAssistanceTemp   
    # OtherBenefitsSource OtherBenefitsSourceIdentify InsuranceFromAnySource  Medicaid    NoMedicaidReason   
    # Medicare    NoMedicareReason    SCHIP   NoSCHIPReason   VAMedicalServices   NoVAMedReason   EmployerProvided 
    #   NoEmployerProvidedReason    COBRA   NoCOBRAReason   PrivatePay  NoPrivatePayReason  StateHealthIns 
    # NoStateHealthInsReason  IndianHealthServices    NoIndianHealthServicesReason    OtherInsurance 
    # OtherInsuranceIdentify 


    DataCollectionStages = incomebenefitsfile['DataCollectionStage'].unique()
    DataCollectionStages = np.setdiff1d(DataCollectionStages, np.array([2, 5]))

    incomebenefitsfile.sort_values(['InformationDate'], ascending=False, inplace=True)

    Columns = ['IncomeFromAnySource', 'TotalMonthlyIncome', 'Earned', 'EarnedAmount',   
               'Unemployment', 'UnemploymentAmount', 'SSI', 'SSIAmount', 'SSDI', 'SSDIAmount',
               'VADisabilityService', 'VADisabilityServiceAmount', 'VADisabilityNonService', 'VADisabilityNonServiceAmount',
               'PrivateDisability', 'PrivateDisabilityAmount', 'WorkersComp', 'WorkersCompAmount', 'TANF', 'TANFAmount', 'GA',
               'GAAmount', 'SocSecRetirement', 'SocSecRetirementAmount', 'ChildSupport', 'ChildSupportAmount',
               'Alimony', 'AlimonyAmount', 'OtherIncomeSource', 'OtherIncomeAmount', 'OtherIncomeSourceIdentify',
               'BenefitsFromAnySource', 'SNAP', 'WIC', 'TANFChildCare', 'TANFTransportation', 'OtherTANF',
               'RentalAssistanceOngoing', 'RentalAssistanceTemp', 'OtherBenefitsSource', 'OtherBenefitsSourceIdentify',
               'InsuranceFromAnySource', 'Medicaid', 'NoMedicaidReason', 'Medicare', 'NoMedicareReason', 'SCHIP', 'NoSCHIPReason',
               'VAMedicalServices', 'NoVAMedReason', 'EmployerProvided', 'NoEmployerProvidedReason', 'COBRA', 'NoCOBRAReason',
               'PrivatePay', 'NoPrivatePayReason', 'StateHealthIns', 'NoStateHealthInsReason', 'IndianHealthServices',
               'NoIndianHealthServicesReason', 'OtherInsurance', 'OtherInsuranceIdentify']

    for c in DataCollectionStages:
        if c == 1:
            col = [1]
            Column = 'AtEntry'
        elif c == 3:
            col = [3]
            Column = 'AtExit'

        for ColName in Columns:
            incomebenefitsfile[ColName + Column] = 0

            Value = incomebenefitsfile.ix[(incomebenefitsfile['DataCollectionStage'].isin(col)), ['ProjectEntryID', ColName]]
            Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
            Value.set_index(['ProjectEntryID'], inplace=True)
            ValueDict = Value.to_dict('dict').values()[0]
       
            incomebenefitsfile[ColName + Column] = incomebenefitsfile['ProjectEntryID'].map(ValueDict)

    collapsedfile = pd.merge(collapsedfile, incomebenefitsfile, how='left', on='ProjectID', copy=False)
    #Services
    #This is more complicated and more like disabilities

    #First, get the records for contact -- they are different from the other ones.

    servicesfile.sort_values(['DateProvided'], ascending=False, inplace=True)
    Value = servicesfile.ix[(servicesfile['RecordType'] == 12), ['ProjectEntryID', 'DateProvided']]
    Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
    Value.set_index(['ProjectEntryID'], inplace=True)
    ValueDict = Value.to_dict('dict').values()[0]
    servicesfile['LastContactDate'] = disabilitiesfile['ProjectEntryID'].map(ValueDict)

    Value = servicesfile.ix[(servicesfile['RecordType'] == 12), ['ProjectEntryID', 'TypeProvided']]
    Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
    Value.set_index(['ProjectEntryID'], inplace=True)
    ValueDict = Value.to_dict('dict').values()[0]
    servicesfile['ContactType'] = disabilitiesfile['ProjectEntryID'].map(ValueDict)

    # Next, get each of the services: 
    for t in range[1, 26]:          
        if t == 1:
            column = 'BasicSupportServices'
        elif t == 2:
            column = 'CommunityServiceServiceLearning'
        elif t == 3:
            column = 'CounselingTherapy'
        elif t == 4:
            column = 'DentalCare'
        elif t == 5:
            column = 'Education'
        elif t == 6:
            column = 'EmploymentAndTrainingServices'
        elif t == 7:
            column = 'CriminalJusticeLegalServices'         
        elif t == 8:
            column = 'LifeSkillsTraining'            
        elif t == 9:
            column = 'ParentingEducationForParentOfYouth'
        elif t == 10:
            column = 'ParentingEducationForYouthWithChildren'
        elif t == 11:
            column = 'Peer(Youth)Counseling'
        elif t == 12:
            column = 'PostNatalCare'
        elif t == 13:
            column = 'PrenatalCare'
        elif t == 14:
            column = 'HealthMedicalCare'            
        elif t == 15:
            column = 'PsychologicalOrPsychiatricCare'
        elif t == 16:
            column = 'RecreationalActivities'
        elif t == 17:
            column = 'SubstanceAbuseAssessmentAndOrTreatment'
        elif t == 18:
            column = 'SubstanceAbusePrevention'
        elif t == 19:
            column = 'SupportGroup'
        elif t == 20:
            column = 'PreventativeOvernightInterimRespite'            
        elif t == 21:
            column = 'PreventativeFormalPlacementInAnAlternativeSettingOutsideOfBCP'
        elif t == 22:
            column = 'PreventativeEntryIntoBCPAfterPreventativeServices'
        elif t == 23:
            column = 'StreetOutreachHealthAndHygieneProductsDistributed'
        elif t == 24:
            column = 'StreetOutreachFoodAndDrinkItems'
        elif t == 25:
            column = 'StreetOutreachServicesInfoBrochures'
    
        servicesfile[column] = 0
        Value = servicesfile.ix[((servicesfile['RecordType'] == 142) & (servicesfile['TypeProvided'] == t)), 'ProjectEntryID']
        Value[column] = 1
        Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
        Value.set_index(['ProjectEntryID'], inplace=True)
        ValueDict = Value.to_dict('dict').values()[0]
        servicesfile[column] = servicesfile['ProjectEntryID'].map(ValueDict)

    #Now, do the same thing for referrals
    for t in range[1, 18]:          
        if t == 1:
            column = 'ChildCareNonTANF'
        elif t == 2:
            column = 'SNAPFoodStamps'
        elif t == 3:
            column = 'EducationMcKinneyVento'
        elif t == 4:
            column = 'HUDSection8OrOtherPermanentHousing'            
        elif t == 5:
            column = 'IndividualDevelopmentAccount'
        elif t == 6:
            column = 'Medicaid'
        elif t == 7:
            column = 'MentoringProgramOtherThanRHYAgency'
        elif t == 8:
            column = 'NationalService'         
        elif t == 9:
            column = 'NonResidentialSubstanceAbuseOrMentalHealth'            
        elif t == 10:
            column = 'OtherFederalStateLocalProgram'
        elif t == 11:
            column = 'PrivateNonProfitCharityOrFoundationSupport'
        elif t == 12:
            column = 'SCHIP'
        elif t == 13:
            column = 'SSISSDIOrOtherDisabilityInsurance'
        elif t == 14:
            column = 'TANF'
        elif t == 15:
            column = 'UnemploymentInsurance'            
        elif t == 16:
            column = 'WIC'
        elif t == 17:
            column = 'WorkforceDevelopmentWIOA'
        
        servicesfile[column] = 0
        Value = servicesfile.ix[((servicesfile['RecordType'] == 162) & (servicesfile['TypeProvided'] == t)), 'ProjectEntryID']
        Value[column] = 1
        Value.drop_duplicates(subset='ProjectEntryID', inplace=True)
        Value.set_index(['ProjectEntryID'], inplace=True)
        ValueDict = Value.to_dict('dict').values()[0]
        servicesfile[column] = servicesfile['ProjectEntryID'].map(ValueDict)

    #EnrollmentCoC.csv - Just the CoCcode
    collapsedfile = pd.merge(collapsedfile, enrollmentcocfile, how='left', on='ProjectID', copy=False)

    #Project.csv
    collapsedfile = pd.merge(collapsedfile, projectfile, how='left', on='ProjectID', copy=False)


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



    collapsedfile.to_csv('CollapsedFile.csv', index=False, header=True, sep=',')

    return HttpResponse(collapsedfile)