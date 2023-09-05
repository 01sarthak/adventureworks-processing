# Databricks notebook source
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Below are some common constants captured that are captured in variables

landingLayer = 'Landing'
rawLayer = 'Raw'
certifiedLayer = 'Certified' 
loggingWithErrorDeltaTablePath = '/mnt/data/adventureworks/LoggingWithError/'
sourceSystemPath = '/mnt/data/sourcefiles/'
onlyTimeinHHMMSS = datetime.now().strftime("%H%M%S")

sourceLandingPath = '/mnt/data/adventureworks/'+landingLayer
rawLandingPath = '/mnt/data/adventureworks/'+rawLayer

subDirectoryPathWithDateTime = '/'+str(datetime.now().year) + '/' + str(datetime.now().month).rjust(2, '0') + '/' + str(datetime.now().day)+'/'

configurations = {
    "Address": {
        "schema": StructType([
            StructField("FILEDATA.AddressID", IntegerType(), True),
            StructField("AddressLine1", StringType(), True),
            StructField("AddressLine2", StringType(), True),
            StructField("City",StringType(), True),
            StructField("StateProvince",StringType(), True),
            StructField("CountryRegion",StringType(), True),
            StructField("PostalCode",StringType(), True)
        ]) 
    }}

entity_schemas = {
    "Address": "array<struct<AddressID:int, AddressLine1:string, City:string, StateProvince:string, CountryRegion:string, PostalCode:string, rowguid:string, ModifiedDate:string>>",
   "Customer": "array<struct<CustomerID:int, NameStyle:string, Title:string, FirstName:string,MiddleName:string, LastName:string, CompanyName:string, SalesPerson:string, EmailAddress:string, Phone:string, rowguid:string, ModifiedDate:string>>",
}
#print('target path for file copy : '+ sourceLandingPath+subDirectoryPathWithDateTime)
