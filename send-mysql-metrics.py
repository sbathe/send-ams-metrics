#!/usr/local/bin/python3
import requests, datetime, json
import mysql.connector as mysql
import config

# Connect to db and get db variables, flatten and merge that
db = mysql.connect(
    host = config.host,
    user = config.user,
    passwd = config.passwd
    )
variables_stmt = 'show global variables'
status_stmt = 'show global status'
cursor = db.cursor()

now = datetime.datetime.now()
epoch = datetime.datetime.utcfromtimestamp(0)
timestamp = (now-epoch).total_seconds()*1000

cursor.execute(variables_stmt)
variables = dict(cursor.fetchall())
cursor.execute(status_stmt)
status = dict(cursor.fetchall())
variables.update(status)

# Ambari Metric System prepare data
now = datetime.datetime.now()
epoch = datetime.datetime.utcfromtimestamp(0)
starttime = timestamp

mlist = []
for k,v in variables.items():
    try:
        float(v)
        metric_tmpl = {}
        metric_tmpl["appid"] = "mysqld"
        metric_tmpl['metricname'] = "MySQL.{0}".format(k)
        metric_tmpl['hostname'] = config.host
        metric_tmpl['timestamp'] = int(timestamp)
        metric_tmpl['starttime'] = int(timestamp)
        metric_tmpl['metrics'] = { repr(int(timestamp)): v}
        mlist.append(metric_tmpl)
    except:
        pass

metrics_to_post = { "metrics": mlist }

# Metrics collctor call
metrics_collector_host = config.collector_host
protocol = config.collector_protocol
headers = {'content-type': 'application/json', 'Accept-Encoding': 'deflate'}
#headers = {'content-type': 'application/json' }
url = '{0}://{1}:6188/ws/v1/timeline/metrics'.format(protocol,metrics_collector_host)
print(json.dumps(metrics_to_post))

r = requests.post(url, data = json.dumps(metrics_to_post), headers = headers)
