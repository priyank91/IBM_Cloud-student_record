# 
# Author: Priyank Arora
# UTA ID: 1001553349
# Assignment/Quiz 1 
# 
#

import os
from flask import Flask,redirect,render_template,request
import urllib
import json
import ibm_db
import ibm_boto3
from ibm_botocore.client import Config

app = Flask(__name__)
''' Getting the GLOBAL Variable for credentionals and related informations.'''
# get service information if on IBM Cloud Platform


if 'VCAP_SERVICES' in os.environ:
    ''' We are reading credentials of dashDB For Transactions first instance attached to flask app'''
    db2info = json.loads(os.environ['VCAP_SERVICES'])['dashDB For Transactions'][0]
    db2cred = db2info["credentials"]
    ''' We are reading credentials of cloud-object-storage first instance attached to flask app'''
    cosinfo = json.loads(os.environ['VCAP_SERVICES'])['cloud-object-storage'][0]
    coscred = cosinfo["credentials"]
    auth_endpoint = 'https://iam.bluemix.net/oidc/token'
    service_endpoint = 'https://s3-api.us-geo.objectstorage.softlayer.net'

else:
    raise ValueError('Cloud Services not added/found in environment')

def cos_imageSearch(image):
    cos = ibm_boto3.resource('s3',
                      ibm_api_key_id=coscred['apikey'],
                      ibm_service_instance_id=coscred['resource_instance_id'],
                      ibm_auth_endpoint=auth_endpoint,
                      config=Config(signature_version='oauth'),
                      endpoint_url=service_endpoint)
    for bucket in cos.buckets.all():
        for obj in bucket.objects.all():
            if (obj.key.lower() == image.lower()):
                cos.Bucket(bucket.name).download_file(obj.key, image)
                return True
    return False

def cos_deleteImage(image):
    cos = ibm_boto3.resource('s3',
                      ibm_api_key_id=coscred['apikey'],
                      ibm_service_instance_id=coscred['resource_instance_id'],
                      ibm_auth_endpoint=auth_endpoint,
                      config=Config(signature_version='oauth'),
                      endpoint_url=service_endpoint)
    for bucket in cos.buckets.all():
        for obj in bucket.objects.all():
            if (obj.key == image):
                obj.delete(obj.key)
                return True
    return False

def cos_uploadImage(image):
    cos = ibm_boto3.resource('s3',
                      ibm_api_key_id=coscred['apikey'],
                      ibm_service_instance_id=coscred['resource_instance_id'],
                      ibm_auth_endpoint=auth_endpoint,
                      config=Config(signature_version='oauth'),
                      endpoint_url=service_endpoint)
    for bucket in cos.buckets.all():
        cos.Bucket(bucket.name).upload_file("./"+image, image)

# main page to dump some environment information
@app.route('/')
def index():
   return render_template('index.html')

@app.route('/Query1', methods=['GET'])
def imageSearch():
    student = request.args.get('student')
    sql="select name, picture from people where name=%s" %('\''+student+'\'')
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2conn:
        stmt = ibm_db.prepare(db2conn,sql)
        ibm_db.execute(stmt)
        result = ibm_db.fetch_assoc(stmt)
        ibm_db.close(db2conn)
        if cos_imageSearch(result["PICTURE"]) == True:
            return render_template("showImage.html", results=result)    
        return render_template("failure.html")

@app.route('/Query2', methods=['GET'])
def imageGradewise():
    grades = request.args.get('grades')
    sql="select name, picture from people where grade < %s" %(grades)
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    rows = []
    while result != False:
        rows.append(result.copy())
        result = ibm_db.fetch_assoc(stmt)
    ibm_db.close(db2conn)
    return render_template("showImage.html", results = rows)

@app.route('/Query3', methods=['GET'])
def updateImage():
    student = request.args.get('student')
    picture = request.args.get('picture')
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    sql = "select picture from people where name=%s" %('\''+student+'\'')
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    if result:
        cos_deleteImage(result["PICTURE"])
    cos_uploadImage(picture)
    sql = "update people set PICTURE=%s where name=%s"%('\''+picture+'\'', '\''+student+'\'') 
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    sql = "select student, picture from people where name=%s"%('\''+student+'\'') 
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    ibm_db.close(db2conn)
    return render_template("city.html", results=result)
    

@app.route('/Query4', methods=['GET'])
def deleteRecord():
    student = request.args.get('student')
    sql="select picture from people where name=%s" %('\''+student+'\'')
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    if result["PICTURE"]:
        cos_deleteImage(result["PICTURE"])
    sql="delete from people where name=%s" %('\''+student+'\'')
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    sql="select * from people"
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    rows = []
    while result != False:
        rows.append(result.copy())
        result = ibm_db.fetch_assoc(stmt)
    ibm_db.close(db2conn)
    return render_template("cityfull.html", results=rows)

@app.route('/Query5', methods=['GET'])
def updateKeyword():
    student = request.args.get('student')
    keyword = request.args.get('keyword')
    sql="update people set KEYWORDS=%s where name=%s"%('\''+keyword+'\'', '\''+student+'\'')
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    sql="select name, keywords from people where name=%s"%('\''+student+'\'')
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    ibm_db.close(db2conn)
    return render_template('city.html', results=result)

@app.route('/Query6', methods=['GET'])
def updateGrades():
    student = request.args.get('student')
    grades = request.args.get('grades')
    sql="update people set grade=%s where name=%s"%('\''+grades+'\'', '\''+student+'\'')
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    sql="select name, grade from people where name=%s"%('\''+student+'\'')
    stmt = ibm_db.prepare(db2conn,sql)
    ibm_db.execute(stmt)
    result = ibm_db.fetch_assoc(stmt)
    ibm_db.close(db2conn)
    return render_template('city.html', results=result)

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
	app.run(host='0.0.0.0', port=int(port))
