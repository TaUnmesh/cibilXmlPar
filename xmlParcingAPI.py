## Part1: Importing Libraries and files
import base64
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
import re
from flask import Flask, request, abort #import main Flask class and request object

s3 = boto3.resource('s3', aws_access_key_id='AKIAVVPFH3IRPFEKIHWS', \
                    aws_secret_access_key='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')

import xml_json
# os.chdir("C:\\Users\\ta_unmesh\\Documents\\XML_Parcing")

#Part2:
app = Flask(__name__)

@app.route('/putXML', methods=['POST']) #GET requests will be blocked
def putXML():
    req_data = request.get_json()
    customerId = req_data["request"]["customerId"]
    encoding = req_data["request"]["encoding"]
    
    if(not req_data):
        abort(400, 'Request Not In JSON Format')
    if (not customerId):
        abort(400, 'CustomerID absent')
    
    xml = req_data["request"]["cibilReport"]
    if encoding == "base64":
        xml = base64.b64decode(xml)
    """
    if len(re.findall(r"xml", xml[0:100])[0]) == 0 :
        abort(400, 'Improper Encoding')
    """
    op_json = xml_json.getCibilJSON(xml)
    s3object = s3.Object("srei-data", "ECL/testXML/"+customerId+".json")
    s3object.put(
        Body=(bytes(json.dumps(op_json).encode('UTF-8')))
    )
    return "Write Operation Successful", 200

@app.route('/getXML', methods=['POST']) #GET requests will be blocked
def getXML():
    req_data = request.get_json()
    customerId = req_data["request"]["customerId"]
    if (not customerId):
        abort(400, 'CustomerID absent')
    try :
        content_object = s3.Object("srei-data", "ECL/testXML/"+customerId+".json")
        file_content = content_object.get()['Body'].read().decode('UTF-8')
    except :
        abort(400, "File is not present for "+customerId)
        file_content = json.dumps(file_content)
    return file_content, 200


if __name__ == '__main__':
    app.run(debug=True, port=5000) #run app in debug mode on port 5000
