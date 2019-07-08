#!/usr/local/bin/python3
import requests, datetime, json
import mysql.connector as mysql
import config

# Connect to db and get db status, flatten and merge that
db = mysql.connect(
    host = config.host,
    user = config.user,
    passwd = config.passwd
    )
status_stmt = 'show global status'
cursor = db.cursor()

per_schema_runtime = 'SELECT schema_name, SUM(count_star) count, ROUND((SUM(sum_timer_wait) / SUM(count_star)) / 1000000) AS avg_microsec FROM performance_schema.events_statements_summary_by_digest WHERE schema_name IS NOT NULL GROUP BY schema_name;'
per_schema_errors = 'SELECT schema_name, SUM(sum_errors) err_count FROM performance_schema.events_statements_summary_by_digest WHERE schema_name IS NOT NULL GROUP BY schema_name;'

keys_required = [ 'Questions', 'Com_select',
                 'Com_insert', 'Com_update', 'Com_delete', 'Slow_queries',
                 'Threads_connected', 'Threads_running',
                 'Connection_errors_internal', 'Aborted_connects',
                 'Connection_errors_max_connections',
                 'Innodb_buffer_pool_pages_total', 'Innodb_page_size',
                 'Innodb_buffer_pool_pages_free',
                 'Innodb_buffer_pool_read_requests', 'Innodb_buffer_pool_reads' ]
now = datetime.datetime.now()
epoch = datetime.datetime.utcfromtimestamp(0)
timestamp = (now-epoch).total_seconds()*1000

cursor.execute(status_stmt)
status = dict(cursor.fetchall())
metrics = dict()
for k in keys_required:
    metrics[k] = status[k]
metrics['Innodb_buffer_pool_pages_total_bytes'] = metrics['Innodb_buffer_pool_pages_total'] * metrics['Innodb_page_size']
metrics['buffer_pool_usage_ratio'] = (metrics['Innodb_buffer_pool_pages_total'] - metrics['Innodb_buffer_pool_pages_free'])/metrics['Innodb_buffer_pool_pages_total']
metrics['mysql_writes'] = metrics['Com_insert'] + metrics['Com_update'] + metrics['Com_delete']

cursor.execute(per_schema_errors)
errors = dict(cursor.fetchall())
for k,v in errors.items():
    metrics['_'.join([k,'errors'])] = float(v)

cursor.execute(per_schema_runtime)
runtime_columns = cursor.column_names
runtime_data = cursor.fetchall()
for rc in range(len(runtime_data)):
    for c in range(len(runtime_columns)-1):
        metrics['.'.join([runtime_data[rc][0],runtime_columns[c+1]])] = float(runtime_data[rc][c+1])


# Ambari Metric System prepare data
now = datetime.datetime.now()
epoch = datetime.datetime.utcfromtimestamp(0)
starttime = timestamp

mlist = []
for k,v in metrics.items():
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
print(metrics_to_post)

r = requests.post(url, data = json.dumps(metrics_to_post), headers = headers)
