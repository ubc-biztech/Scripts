# Copyright 2021 UBC BizTech or its affiliates. All Rights Reserved.
# Written by: Marco Ser
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.


import pandas as pd
import json
import boto3
from datetime import datetime, timedelta

def main():
    # Enable the commands below to visually see all the outputs from pandas
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)


    df = pd.read_csv("./000.csv")

    # Filter by UBC student, University Student, High school student, None of the above
    groups = df.groupby(df["Please choose the option that's most relevant to you"])
    
    # Get the dataframe for those groups
    ubc = groups.get_group("I am a current/prospective UBC student") # UBC
    uni = groups.get_group("I am a current/prospective university student") # University
    hs = groups.get_group("I am a high school student") # High School
    na = groups.get_group("None of the above") # None of the above

    # Remove columns that have NaN values 
    ubc = ubc[ubc.columns[~ubc.isnull().all()]]
    uni = uni[uni.columns[~uni.isnull().all()]]
    hs = hs[hs.columns[~hs.isnull().all()]]
    na = na[na.columns[~na.isnull().all()]]

    # Rename columns
    ubc = ubc.rename(columns={"Timestamp":"timestamp",
                              "Username": "email",
                              "Please choose the option that's most relevant to you": "education",
                              "First Name": "first_name",
                              "Last Name": "last_name",
                              "What are your preferred pronouns?": "pronouns",
                              "UBC Student Number": "student_number",
                              "Academic Year Level": "year",
                              "Faculty": "faculty",
                              "Major": "major",
                              "Were you a BizTech member last year?": "prev_member",
                              "Are you an international student?": "international",
                              "What topics did you want to see the most discussed in the future? ": "topics",
                              "How did you hear about us?": "heard_from"})

    uni = uni.rename(columns={"Timestamp":"timestamp",
                            "Username": "email",
                            "Please choose the option that's most relevant to you": "education",
                            "First Name.1": "first_name",
                            "Last Name.1": "last_name",
                            "What are your preferred pronouns?.1": "pronouns",
                            "What university do you currently attend?": "university",
                            "Academic Year Level.1": "year",
                            "Faculty.1": "faculty",
                            "Major.1": "major",
                            "Were you a BizTech member last year?.1": "prev_member",
                            "Are you an international student?.1": "international",
                            "What topics did you want to see the most discussed in the future? .1": "topics",
                            "How did you hear about us?.1": "heard_from"})

    hs = hs.rename(columns={"Timestamp":"timestamp",
                        "Username": "email",
                        "Please choose the option that's most relevant to you": "education",
                        "First Name.2": "first_name",
                        "Last Name.2": "last_name",
                        "What are your preferred pronouns?.2": "pronouns",
                        "What high-school do you currently attend?": "high_school",
                        "Academic Grade": "year",
                        "How did you hear about us?.2": "heard_from"})

    na = na.rename(columns={"Timestamp":"timestamp",
                              "Username": "email",
                              "Please choose the option that's most relevant to you": "education",
                              "First Name": "first_name",
                              "Last Name": "last_name",
                              "What are your preferred pronouns?": "pronouns",
                              "UBC Student Number": "student_number",
                              "Academic Year Level": "year",
                              "Faculty": "faculty",
                              "Major": "major",
                              "Were you a BizTech member last year?": "prev_member",
                              "Are you an international student?": "international",
                              "What topics did you want to see the most discussed in the future? ": "topics",
                              "How did you hear about us?": "heard_from"})
    
    members = pd.concat([ubc,uni,hs,na],ignore_index=True)

    # Cleans up the data before we store to dynamo
    members = members.reset_index()
    members = members.rename(columns={"index": "id"})
    members['id'] = members['id']
    members['timestamp'] = members['timestamp'].apply(lambda x: int(convert_to_timestamp(x)))
    members['email'] = members['email'].apply(lambda x: x.strip())
    members['education'] = members['education'].apply(lambda x: convert_education(x))
    members['first_name'] = members['first_name'].apply(lambda x: x.strip())
    members['last_name'] = members['last_name'].apply(lambda x: x.strip())
    members['pronouns'] = members['pronouns'].fillna("").apply(lambda x: x.strip())
    members['student_number'] = members['student_number'].fillna(0).apply(lambda x: int(x))
    members['year'] = members['year'].apply(lambda x: x.replace('Year','').strip())
    members['faculty'] = members['faculty'].fillna("").apply(lambda x: x.strip())
    members['major'] = members['major'].fillna("").apply(lambda x: x.strip())
    members['prev_member'] = members['prev_member'].map({'Yes': True, 'No': False})
    members['international'] = members['international'].map({'Yes': True, 'No': False})
    members['topics'] = members['topics'].apply(lambda x: (str(x).split(';')))
    members['heard_from'] = members['heard_from'].apply(lambda x: x.strip())
    members['university'] = members['university'].fillna("").apply(lambda x: x.strip())
    members['high_school'] = members['high_school'].fillna("").apply(lambda x: x.strip())

    # Convert records to JSON String
    response = members.to_json(orient='records')

    # Converts to JSON Object
    response = json.loads(response)

    # Removes empty columns in JSON Object (required to upload to DynamoDB)
    for index,item in enumerate(response):
        response[index] = {k:v for k,v in item.items() if v is not None}

    
    upload_to_dynamo(response)

#Uploads response to DynamoDB
def upload_to_dynamo(response):
    # Initialize BOTO3 client
    client = boto3.Session(
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name = ""
    )

    # Initialize DynamoDb client and assign it the table
    dynamodb = client.resource('dynamodb')
    table = dynamodb.Table('biztechMemberships2021')

    # Using a batch writer to batch the put_item requests
    with table.batch_writer() as batch:
        for item in response:
            print(item)
            batch.put_item(Item=item)
 

# Converts education to the proper ENUM
def convert_education(education):
    if(education == "I am a current/prospective UBC student"):
        return 'UBC'
    elif(education == "I am a current/prospective university student"):
        return 'UNI'
    elif(education == "I am a high school student"):
        return 'HS'
    else:
        return 'NA'

# Convert date format to readable format 
# We subtract one hour since we are currently in PST which is 1 hour behind MDT
# date returns back local time
def convert_to_timestamp(date):
    date = datetime.strptime(date[:-4], "%Y/%m/%d %I:%M:%S %p") - timedelta(hours=1)
    timestamp = datetime.timestamp(date)
    return timestamp

if __name__ == "__main__":
    main()