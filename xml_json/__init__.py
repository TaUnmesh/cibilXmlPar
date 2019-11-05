import pandas as pd
import numpy as np
import datetime
import xml.etree.ElementTree as ET
import base64
import json
import os
import re
import datetime
import functools
import boto3

# Load All Mapper Files
cibilScoreMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name= "CibilScore")
phoneTypeMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "PhoneType", dtype = {"Symbol" : str})
regionMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "RegionCode", dtype = {"Region_Code" : str})
addressTypeMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "AddressType", dtype = {"Address_Type_Code" : str})
addressOwnershipTypeMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "OwnershipType", dtype = {"Address_Ownership_Code" : str})
accountDesignerMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "AccountDesignator", dtype = {"Account_Designator_Code" : str})
accountTypeMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "AccountType", dtype = {"Account_Type_Code" : str})
collateralTypeMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "CollateralType", dtype = {"Collateral_Type_Code" : str})
paymentFrequencyMapping = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "PaymentType", dtype = {"Payment_Frequency_Code" : str})
DPDMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "DPD")
lossTypeMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "LossType")
occupationMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "Occupation", dtype = {"Occupation_Code" : str})
incomeFrequencyMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "IncomeFrequency")
grossNetMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "GrossNet")
repaymentTrackMapper = pd.read_excel("..\\XMLMapperFile.xlsx", sheet_name = "RepaymentTrack")

# All global functions
#1
def getSelectedChild(parent,child_tag) :
    for child in parent :
        if ((len(re.findall(pattern="^{.*}"+child_tag+"$", string= child.tag))) > 0) or ((len(re.findall(pattern="^"+child_tag+"$", string= child.tag))) > 0) :
                return child
ns4 = "com/truelink/ds/sch/report/truelink/v5"
tag_ns4 = "{"+"com/truelink/ds/sch/report/truelink/v5"+"}"
ns2 = "com/truelink/ds/sch/pii/v1"
tag_ns2 = "{"+"com/truelink/ds/sch/pii/v1"+"}"

#2
def getDateFromDateTimeString(ip):
    try:
        op = ip[:10]
    except:
        op = ""
    return(op)

#3
def myCleanStr(myStr):
    myStr = myStr.upper()
    myStr = re.sub(r"^[\s+]", "", re.sub(r"[\s+]$", "", myStr))
    return(myStr)

#4
def getSeriesFirstElement(mySer):
    try:
        myOp = mySer.tolist()[0]
    except:
        myOp = ""
    return myOp

#5
def getTextAsString(tagObject):
    try:
        opString = tagObject.text
        opString = myCleanStr(opString)
        if not opString:
            opString = ""
    except:
        opString = ""
    return(opString)

#6
def getAttributeAsString(tagObject, attributeName):
    try:
        opString = tagObject.attrib[attributeName]
        opString = myCleanStr(opString)
        if not opString:
            opString = ""
    except:
        opString = ""
    return(opString)



# Functions for data Serialisation
def getStringFromTimeStamp(timeStamp):
    try :
        op = timeStamp.strftime("%Y-%m-%d")
    except :
        op = "NA"
    return(op)

def getFloatFromNpFloat(myInt):
    try :
        op = float(myInt)
    except :
        op = "NA"
    return(op)

def getIntFromNpInt(myInt):
    try :
        op = int(myInt)
    except :
        op = "NA"
    return(op)


def getCibilJSON(xml):
    # 0 Reading XML
    root = ET.fromstring(xml)

    CreditReport = getSelectedChild(
        getSelectedChild(
            getSelectedChild(
                getSelectedChild(
                    getSelectedChild(root,
                                     "Body"),
                    "GetCustomerAssetsResponse"),
                "GetCustomerAssetsSuccess"),
            "Asset"),
        "TrueLinkCreditReport")

    # 1 Borrower
    borrower = getSelectedChild(CreditReport, "Borrower")
    op_borrower = dict()

    if borrower:
        # Borrower Name
        forname = getTextAsString(
            borrower.find(tag_ns4 + "BorrowerName/" + tag_ns4 + "Name/" + tag_ns2 + "Forename")
        )
        surname = getTextAsString(
            borrower.find(tag_ns4 + "BorrowerName/" + tag_ns4 + "Name/" + tag_ns2 + "Surname")
        )

        op_borrower["borrowerName"] = myCleanStr(forname + " " + surname)

        # Borrower DOB
        dob = getDateFromDateTimeString(
            getAttributeAsString(borrower.find(tag_ns4 + "Birth"), "date")
        )
        op_borrower["borrowerDOB"] = pd.to_datetime(dob, errors="coerce")

        # Borrower DOB
        gender = getTextAsString(
            borrower.find(tag_ns4 + "Gender")
        )
        op_borrower["borrowerGender"] = myCleanStr(gender)

        # Borrower Emails
        email_list = list()
        for email in borrower.findall(tag_ns4 + "EmailAddress"):
            email1 = getTextAsString(
                email.find(tag_ns4 + "Email")
            )
            email_list.append(email1)
        op_borrower["emails"] = email_list

        # Credit Scores
        op_borrower["riskScore_D2C"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            borrower.find(tag_ns4 + "CreditScore"), "riskScore"
        )), errors="coerce")

    op_borrower = op_borrower

    # 2 phoneList
    phListObj = borrower.findall(tag_ns4 + "BorrowerTelephone")
    phoneList = list()
    for ph in phListObj:
        phoneDict = None
        phoneDict = dict()

        phoneDict["phoneNumber"] = getTextAsString(ph.find(tag_ns4 + "PhoneNumber" + "/" + tag_ns2 + "Number"))
        pType = getAttributeAsString(
            ph.find(tag_ns4 + "PhoneType"), "symbol"
        )
        phoneDict["phoneType"] = myCleanStr(
            getSeriesFirstElement(phoneTypeMapping.loc[phoneTypeMapping.Symbol == pType, "Phone_Type"])
        )
        phoneList.append(phoneDict)

    phoneNumbers = phoneList

    # 3 varificationIDs
    identifier = borrower.findall(tag_ns4 + "IdentifierPartition" + "/" + tag_ns4 + "Identifier")
    respIdent = list()
    for iden in identifier:
        tempIdent = dict()

        tempIdent["type"] = myCleanStr(getTextAsString(iden.find(tag_ns4 + "ID" + "/" + tag_ns2 + "IdentifierName")))
        tempIdent["number"] = myCleanStr(getTextAsString(iden.find(tag_ns4 + "ID" + "/" + tag_ns2 + "Id")))
        tempIdent["issueDate"] = pd.to_datetime(getTextAsString(iden.find(tag_ns4 + "Source/" + tag_ns4 + "IssueDate")), \
                                                errors="coerce")
        tempIdent["expiryDate"] = pd.to_datetime(
            getTextAsString(iden.find(tag_ns4 + "Source/" + tag_ns4 + "ExpirationDate")), \
            errors="coerce")

        respIdent.append(tempIdent)

    varificationIDs = respIdent

    # 4 op_address
    op_address = list()
    for add in borrower.findall(tag_ns4 + "BorrowerAddress"):
        addressInfo = dict()

        # Address
        streetAddress = getTextAsString(
            add.find(tag_ns4 + "CreditAddress/" + tag_ns2 + "StreetAddress")
        )

        city = getTextAsString(
            add.find(tag_ns4 + "CreditAddress/" + tag_ns2 + "City")
        )

        postalCode = getTextAsString(
            add.find(tag_ns4 + "CreditAddress/" + tag_ns2 + "PostalCode")
        )

        region = getTextAsString(
            add.find(tag_ns4 + "CreditAddress/" + tag_ns2 + "Region")
        )

        region = getSeriesFirstElement(
            regionMapping.loc[regionMapping.Region_Code == region, "Region"]
        )

        addressInfo["address"] = myCleanStr(streetAddress + " " + city + " " + postalCode + " " + \
                                            region)

        # Address Type
        addressType = getAttributeAsString(
            add.find(tag_ns4 + "Dwelling"), "symbol"
        )
        addressType = getSeriesFirstElement(
            addressTypeMapping.loc[
                addressTypeMapping.Address_Type_Code == addressType,
                "Address_Type"
            ]
        )
        addressInfo["addressType"] = myCleanStr(addressType)

        # Date Reported
        addressInfo["dateReported"] = getDateFromDateTimeString(
            getAttributeAsString(add, "dateReported")
        )
        addressInfo["dateReported"] = pd.to_datetime(addressInfo["dateReported"])

        # Ownership
        ownership = getAttributeAsString(
            add.find(tag_ns4 + "Ownership"),
            "symbol"
        )
        ownership = getSeriesFirstElement(
            addressOwnershipTypeMapping.loc[
                addressOwnershipTypeMapping.Address_Ownership_Code == ownership,
                "Address_Ownership"
            ]
        )
        addressInfo["ownership"] = myCleanStr(ownership)

        op_address.append(addressInfo)

    addressInfo = op_address

    # 5 employerList
    employerList = []
    for emp in borrower.findall(tag_ns4 + "Employer"):
        employer = dict()
        employer["account"] = getAttributeAsString(emp, "account")
        print(employer["account"])
        employer["account"] = myCleanStr(getSeriesFirstElement(
            accountTypeMapping.loc[accountTypeMapping.Account_Type_Code == employer["account"], "Account_Type"]
        ))

        employer["dateReported"] = pd.to_datetime(myCleanStr(getDateFromDateTimeString(
            getAttributeAsString(emp, "dateReported")
        )), errors="coerce")

        employer["income"] = pd.to_numeric(myCleanStr(getAttributeAsString(emp, "income")))

        employer["occupationCode"] = myCleanStr(getAttributeAsString(
            emp.find(tag_ns4 + "OccupationCode"),
            "symbol"
        ))
        employer["occupationCode"] = myCleanStr(getSeriesFirstElement(
            occupationMapper.loc[occupationMapper.Occupation_Code == employer["occupationCode"], "Occupation"]
        ))

        employer["netGrossIndicator"] = myCleanStr(getAttributeAsString(
            emp.find(tag_ns4 + "NetGrossIndicator"),
            "symbol"
        ))
        employer["netGrossIndicator"] = myCleanStr(getSeriesFirstElement(
            grossNetMapper.loc[
                grossNetMapper.Gross_Net_Income_Code == employer["netGrossIndicator"], "Gross_Net_Income"]
        ))

        employer["incomeFreqIndicator"] = myCleanStr(getAttributeAsString(
            emp.find(tag_ns4 + "IncomeFreqIndicator"),
            "symbol"
        ))
        employer["incomeFreqIndicator"] = myCleanStr(getSeriesFirstElement(
            incomeFrequencyMapper.loc[
                incomeFrequencyMapper.Income_Freq_Code == employer["incomeFreqIndicator"], "Income_Freq"]
        ))
        employerList.append(employer)

    employerList = employerList

    # 6 inquiryList
    inquiryList = list()
    for inq in CreditReport.findall(tag_ns4 + "InquiryPartition"):
        inquiry = dict()
        inquiry["subscriberName"] = myCleanStr(getAttributeAsString(inq.find(tag_ns4 + "Inquiry"), "subscriberName"))
        inquiry["inquiryDate"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(
            getAttributeAsString(inq.find(tag_ns4 + "Inquiry"), "inquiryDate"))), errors="coerce")
        inquiry["amount"] = pd.to_numeric(myCleanStr(
            getAttributeAsString(inq.find(tag_ns4 + "Inquiry"), "amount")), errors="coerce")
        inquiry["inquiryType"] = myCleanStr(getAttributeAsString(inq.find(tag_ns4 + "Inquiry"), "inquiryType"))
        inquiry["inquiryType"] = myCleanStr(getSeriesFirstElement(accountTypeMapping.loc[
                                                                      accountTypeMapping["Account_Type_Code"] ==
                                                                      inquiry["inquiryType"],
                                                                      "Account_Type"
                                                                  ]))
        inquiryList.append(inquiry)

    inquiryList = inquiryList

    # 7 accountInfoList
    accountInfoList = list()
    for tradeLine in CreditReport.findall(tag_ns4 + "TradeLinePartition"):
        accountInfo = dict()

        # Phase 1 : Symbols
        accountInfo["accountNumber"] = myCleanStr(
            getAttributeAsString(tradeLine.find(tag_ns4 + "Tradeline"), "accountNumber"))

        accountInfo["accountTypeSymbol"] = myCleanStr(
            getAttributeAsString(tradeLine, "accountTypeSymbol"))
        accountInfo["accountTypeSymbol"] = myCleanStr(getSeriesFirstElement(accountTypeMapping.loc[
                                                                                accountTypeMapping[
                                                                                    "Account_Type_Code"] == accountInfo[
                                                                                    "accountTypeSymbol"],
                                                                                "Account_Type"
                                                                            ]))

        aD = myCleanStr(
            getAttributeAsString(tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "AccountDesignator"), "symbol"))
        accountInfo["AccountDesignator"] = myCleanStr(getSeriesFirstElement(accountDesignerMapping.loc \
                                                                                [
                                                                                accountDesignerMapping.Account_Designator_Code == aD, "Account_Designator"]))

        aD = myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "CollateralType"), "symbol"))
        accountInfo["CollateralType"] = myCleanStr(getSeriesFirstElement(collateralTypeMapping.loc \
                                                                             [
                                                                             collateralTypeMapping.Collateral_Type_Code == aD, "Collateral_Type"]))

        aD = myCleanStr(getAttributeAsString(tradeLine.find(
            tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "PaymentFrequency"), "symbol"))
        accountInfo["paymentFrequency"] = myCleanStr(getSeriesFirstElement(paymentFrequencyMapping.loc \
                                                                               [
                                                                               paymentFrequencyMapping.Payment_Frequency_Code == aD, "Payment_Frequency"]))

        accountInfo["interestRate"] = pd.to_numeric(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "interestRate"), errors="coerce")
        accountInfo["termMonths"] = pd.to_numeric(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "termMonths"), errors="coerce")

        # Phase 2 : Dates
        accountInfo["dateOpened"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "dateOpened"))), errors="coerce")
        accountInfo["dateReported"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "dateReported"))), errors="coerce")
        accountInfo["dateClosed"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "dateClosed"))), errors="coerce")

        accountInfo["dateLastPayment"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "dateLastPayment"))), errors="coerce")
        accountInfo["startDate"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "PayStatusHistory"),
            "startDate"))), errors="coerce")
        accountInfo["endDate"] = pd.to_datetime(getDateFromDateTimeString(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "PayStatusHistory"),
            "endDate"))), errors="coerce")

        # Phase 3 : Amounts
        accountInfo["highBalance"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "highBalance")), errors="coerce")
        accountInfo["currentBalance"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "currentBalance")), errors="coerce")
        accountInfo["amountPastDue"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "amountPastDue")), errors="coerce")
        accountInfo["collateral"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "collateral")), errors="coerce")
        accountInfo["actualPaymentAmount"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade"), "actualPaymentAmount")), errors="coerce")
        accountInfo["EMIAmount"] = pd.to_numeric(myCleanStr(getTextAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "EMIAmount"))),
            errors="coerce")

        # Phase 4 : Amounts_2
        accountInfo["writtenOffAmtTotal"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "writtenOffAmtTotal")), errors="coerce")
        accountInfo["writtenOffAmtPrincipal"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "writtenOffAmtPrincipal")), errors="coerce")
        accountInfo["settlementAmount"] = pd.to_numeric(myCleanStr(getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline"), "settlementAmount")), errors="coerce")
        accountInfo["creditLimit"] = pd.to_numeric(myCleanStr(getTextAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "CreditLimit"))),
            errors="coerce")
        accountInfo["cashLimit"] = pd.to_numeric(myCleanStr(getTextAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + tag_ns4 + "CashLimit"))),
            errors="coerce")

        tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + "PayStatusHistory")

        # DPD
        listOfMonthlyPayStatus = tradeLine.findall(tag_ns4 + "Tradeline/" + tag_ns4 + "GrantedTrade/" + \
                                                   tag_ns4 + "PayStatusHistory/" + tag_ns4 + "MonthlyPayStatus")

        def cleanDPD(MonthlyPayStatus):
            MonthlyPayStatus = getAttributeAsString(MonthlyPayStatus, "status")
            if (len(re.sub(r"[A-Za-z]", "", MonthlyPayStatus)) != len(MonthlyPayStatus) and len(MonthlyPayStatus) > 0):
                dpd = getSeriesFirstElement(DPDMapper.loc[DPDMapper.DPD_Code == MonthlyPayStatus, "DPD"])
            else:
                dpd = MonthlyPayStatus
            dpd = pd.to_numeric(dpd, errors="coerce")
            return (dpd)

        def cleanDPD_org(MonthlyPayStatus):
            MonthlyPayStatus = getAttributeAsString(MonthlyPayStatus, "status")
            dpd = MonthlyPayStatus
            return (dpd)

        accountInfo["DPD_36Months"] = list(map(lambda x: cleanDPD(x), listOfMonthlyPayStatus))
        accountInfo["DPD_36Months_org"] = list(map(lambda x: cleanDPD_org(x), listOfMonthlyPayStatus))

        # lossType
        accountConditionAbbreviation = getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "AccountCondition"), "abbreviation"
        )
        accountConditionSymbol = getAttributeAsString(
            tradeLine.find(tag_ns4 + "Tradeline/" + tag_ns4 + "AccountCondition"), "symbol"
        )
        lossType = accountConditionAbbreviation + "_" + accountConditionSymbol

        lossType = getSeriesFirstElement(
            lossTypeMapper.loc[lossTypeMapper.LossType_Code == lossType, "LossType"]
        )
        accountInfo["lossType"] = lossType

        accountInfoList.append(accountInfo)

    # accountInfoList
    accountSummary = dict()
    accountSummary["totalAccounts"] = len(accountInfoList)
    accountSummary["zeroBalanceAccounts"] = len(list(filter(lambda x: x == 0,
                                                            list(map(lambda x: x["currentBalance"], accountInfoList))
                                                            )))
    accountSummary["recentAccountOpenDate"] = functools.reduce(lambda x, y: max(x, y),
                                                               list(map(lambda x: x["dateOpened"], accountInfoList)))
    accountSummary["oldestAccountOpenDate"] = functools.reduce(lambda x, y: min(x, y),
                                                               list(map(lambda x: x["dateOpened"], accountInfoList)))
    accountSummary["totalOverdue"] = pd.Series(list(map(lambda x: x["amountPastDue"],
                                                        accountInfoList))).sum(skipna=True)
    accountSummary = accountSummary

    # 8 Intelligent Variables:
    intelligentVariables = dict()
    intelligentVariables["noEnq"] = len(inquiryList)
    intelligentVariables["avgEnqAmt"] = pd.Series(pd.to_numeric( \
        list(map(lambda x: x["amount"], inquiryList)), errors='coerce')).mean(skipna=True)
    intelligentVariables["maxEnqAmt"] = pd.Series(pd.to_numeric( \
        list(map(lambda x: x["amount"], inquiryList)), errors='coerce')).max(skipna=True)

    latestInquiryDate = pd.Series(list(map(lambda x: x["inquiryDate"], inquiryList))).max(skipna=True)
    intelligentVariables["timeSinceLastEnq"] = (datetime.datetime.today() - latestInquiryDate).days

    intelligentVariables["maxLoanAmt"] = pd.Series(list(map(lambda x: x["highBalance"], accountInfoList))).max(
        skipna=True)
    avgLoanAmt = pd.Series(list(map(lambda x: x["highBalance"], accountInfoList))).mean(skipna=True)
    intelligentVariables["loanToInquiryRatio"] = avgLoanAmt / intelligentVariables["avgEnqAmt"]

    intelligentVariables["netCurrentBalance"] = pd.Series(
        list(map(lambda x: x["currentBalance"], accountInfoList))).sum(skipna=True)
    intelligentVariables["noOfLiveAccounts"] = len(
        list(filter(lambda x: x != 0, map(lambda x: x["currentBalance"], accountInfoList))))

    myIquiryDate = list(pd.Series(list(map(lambda x: x["inquiryDate"], inquiryList))).sort_values())
    intelligentVariables["avgDifferenceEnq"] = pd.Series(
        [x - myIquiryDate[i - 1] for i, x in enumerate(myIquiryDate) if i > 0]).mean(skipna=True)

    def getAvgDPD(listDPD):
        op = pd.Series(listDPD).mean(skipna=True)
        if pd.isna(op):
            op = 0
        return op

    avgListDPD = list(map(lambda x: getAvgDPD(x["DPD_36Months"]), accountInfoList))
    listLoanAmt = list(map(lambda x: x["highBalance"], accountInfoList))
    totalSumProductHighamtDPD = pd.Series(list(map(lambda x, y: x * y, avgListDPD, listLoanAmt))).sum(skipna=True)
    totalLoanAmt = pd.Series(list(map(lambda x: x["highBalance"], accountInfoList))).sum(skipna=True)

    try:
        intelligentVariables["avgDifferenceEnq"] = totalSumProductHighamtDPD / totalLoanAmt
    except:
        intelligentVariables["avgDifferenceEnq"] = np.nan

    intelligentVariables = intelligentVariables

    # 9 varsPE
    varsPE = dict()
    varsPE["avgDelay"] = pd.Series(avgListDPD).mean(skipna=True)
    maxListDPD = list(map(lambda x: pd.Series(x["DPD_36Months"]).max(skipna=True), accountInfoList))
    varsPE["maxDPD"] = pd.Series(maxListDPD).max(skipna=True)
    listLenPayments = list(map(lambda x: len(x["DPD_36Months"]), accountInfoList))
    varsPE["noInstallmentPaid"] = pd.Series(listLenPayments).sum(skipna=True)
    listLenPayments = list(map(lambda x: len(x["DPD_36Months"]), accountInfoList))
    varsPE["noInstallmentPaid"] = pd.Series(listLenPayments).sum(skipna=True)
    listDefaultIndicator = list(
        map(lambda x: 1 if len(list(filter(lambda x: x >= 60, x["DPD_36Months"]))) > 0 else 0, accountInfoList)
    )
    varsPE["noOfDefaults"] = pd.Series(listDefaultIndicator).sum(skipna=True)

    Max_Delay = pd.cut(
        [varsPE["maxDPD"]],
        [-np.Inf, 30, 45, np.Inf],
        right=False,
        labels=["LT_30", "30_45", "GTE_45"]
    )
    Max_Delay = getSeriesFirstElement(pd.Series(Max_Delay))
    Avg_Delay = pd.cut(
        [varsPE["avgDelay"]],
        [-np.Inf, 15, 30, np.Inf],
        right=False,
        labels=["LT_15", "15_30", "GTE_30"]
    )
    Avg_Delay = getSeriesFirstElement(pd.Series(Avg_Delay))
    repaymentTag = Max_Delay + "__" + Avg_Delay
    varsPE["repaymentTractWithoutSREI"] = getSeriesFirstElement(
        repaymentTrackMapper.loc[repaymentTrackMapper["Repayment_Track_Without_SREI_Code"] \
                                 == repaymentTag, "Repayment_Track_Without_SREI"])

    varsPE = varsPE

    # Code for variable serialization
    #1
    for val in op_borrower:
        if type(val) in [np.int, np.int16, np.int32, np.int64]:
            val = getIntFromNpInt(val[key1])
        elif type(val) in [np.float, np.float16, np.float32, np.float64]:
            val = getIntFromNpInt(val)
        elif type(val) == str:
            val = "NA" if not val else val
        elif (type(val) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val)):
            val = getStringFromTimeStamp(val)

    # 2
    for key in op_borrower:
        if type(op_borrower[key]) in [np.int, np.int16, np.int32, np.int64]:
            op_borrower[key] = getIntFromNpInt(op_borrower[key])
        elif type(op_borrower[key]) in [np.float, np.float16, np.float32, np.float64]:
            op_borrower[key] = getIntFromNpInt(op_borrower[key])
        elif type(op_borrower[key]) == str:
            op_borrower[key] = "NA" if not op_borrower[key] else op_borrower[key]
        elif (type(op_borrower[key]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val)):
            op_borrower[key] = getStringFromTimeStamp(op_borrower[key])

    # 3
    for val in varificationIDs:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = getStringFromTimeStamp(val[key1])

    for val in phoneNumbers:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = getStringFromTimeStamp(val[key1])

    for val in addressInfo:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = getStringFromTimeStamp(val[key1])

    # 4
    for val in inquiryList:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = "3_3_3"

    # 5
    for val in employerList:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = getStringFromTimeStamp(val[key1])

    # 6
    for key in accountSummary:
        if type(accountSummary[key]) in [np.int, np.int16, np.int32, np.int64]:
            accountSummary[key] = getIntFromNpInt(accountSummary[key])
        elif type(accountSummary[key]) in [np.float, np.float16, np.float32, np.float64]:
            accountSummary[key] = getFloatFromNpFloat(accountSummary[key])
        elif type(accountSummary[key]) == str:
            accountSummary[key] = "NA" if not accountSummary[key] else accountSummary[key]
        elif (type(accountSummary[key]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(accountSummary[key])):
            accountSummary[key] = getStringFromTimeStamp(accountSummary[key])

    # 7
    for key in intelligentVariables:
        if type(intelligentVariables[key]) in [np.int, np.int16, np.int32, np.int64]:
            intelligentVariables[key] = getIntFromNpInt(intelligentVariables[key])
        elif type(intelligentVariables[key]) in [np.float, np.float16, np.float32, np.float64]:
            intelligentVariables[key] = getFloatFromNpFloat(intelligentVariables[key])
        elif type(intelligentVariables[key]) == str:
            intelligentVariables[key] = "NA" if not intelligentVariables[key] else intelligentVariables[key]
        elif (type(intelligentVariables[key]) == pd._libs.tslibs.timestamps.Timestamp or \
              pd.isna(intelligentVariables[key])):
            intelligentVariables[key] = getStringFromTimeStamp(intelligentVariables[key])

    # 8
    for key in varsPE:
        if type(varsPE[key]) in [np.int, np.int16, np.int32, np.int64]:
            varsPE[key] = getIntFromNpInt(varsPE[key])
        elif type(varsPE[key]) in [np.float, np.float16, np.float32, np.float64]:
            varsPE[key] = getFloatFromNpFloat(varsPE[key])
        elif type(varsPE[key]) == str:
            varsPE[key] = "NA" if not varsPE[key] else varsPE[key]
        elif (type(varsPE[key]) == pd._libs.tslibs.timestamps.Timestamp or np.isna(varsPE[key])):
            varsPE[key] = getStringFromTimeStamp(varsPE[key])

    # 9
    def myListSerialisation(inputList):
        try:
            if (type(inputList[0]) in [np.int, np.int16, np.int32, np.int64]):
                inputList = list(map(int, inputList))
            if (type(inputList[0]) in [np.float, np.float16, np.float32, np.float64]):
                inputList = list(map(int, inputList))
            if (type(inputList[0]) == str):
                inputList = list(map(lambda x: "NA" if not x else x, inputList))
        except:
            inputList = []

        return inputList

    # 10
    for val in accountInfoList:
        for key1 in val:
            if type(val[key1]) in [np.int, np.int16, np.int32, np.int64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) in [np.float, np.float16, np.float32, np.float64]:
                val[key1] = getIntFromNpInt(val[key1])
            elif type(val[key1]) == str:
                val[key1] = "NA" if not val[key1] else val[key1]
            elif (type(val[key1]) == list):
                val[key1] = myListSerialisation(val[key1])
            elif (type(val[key1]) == pd._libs.tslibs.timestamps.Timestamp or pd.isna(val[key1])):
                val[key1] = getStringFromTimeStamp(val[key1])

    # Meriging All List and then converting them in JSON
    op_List = op_borrower
    op_List["varificationIDs"] = varificationIDs
    op_List["phoneNumbers"] = phoneNumbers
    op_List["addressInfo"] = addressInfo
    op_List["addressInfo"] = addressInfo
    op_List["employerList"] = employerList
    op_List["inquiryList"] = inquiryList
    op_List["accountInfoList"] = accountInfoList
    op_List["accountSummary"] = accountSummary
    op_List["intelligentVariables"] = intelligentVariables
    op_List["varsPE"] = varsPE
    op_json = json.dumps(op_List)

    return op_json
