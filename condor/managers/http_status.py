"""@author: Leif Johnson <leif@cs.utexas.edu>"""

import BaseHTTPServer
import condor
import datetime
import socket
import sys
import threading

logger = condor.log.get_logger(__name__, default_level = "INFO")

TEMPLATE = '''\
<!doctype html>
<html>
<head>
<title>UT Condor</title>
<link href="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.2/css/bootstrap-combined.min.css" rel="stylesheet">
<body>
<div class="container">

<div class="row">
<h2 class="span12">UT Condor Status <small>@%(hostname)s</small></h2>
</div>

<div class="row">

<div class="span6"><h3>Tasks: %(remaining_tasks)d remaining of %(all_tasks)d</h3>
<table class="table table-striped table-bordered table-hover table-condensed">
<thead><tr><th>ID<th>Priority<th>Last Update
<tbody>%(task_table)s
</table>
</div>

<div class="span6"><h3>Workers: %(busy_workers)d busy of %(all_workers)d total</h3>
<table class="table table-striped table-bordered table-hover table-condensed">
<thead><tr><th>ID<th>Last Update<th># Assigned Tasks
<tbody>%(worker_table)s
</table>
</div>

</div></div>
'''

def format_unix(ts):
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def serve_http_status(manager):
    '''Serve worker and task status info over HTTP.'''

    tst = manager.tstates
    wst = manager.wstates

    class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ('', '/'):
                self.send_response(404)
                self.end_headers()
                return

            task_table = []
            for k, st in sorted(tst.iteritems(), key=lambda i: i[1].score()):
                rank, ts, _ = st.score()
                tup = ['success', st.task, k, 'finished', 'finished']
                if sys.maxint > rank > 0:
                    tup[0] = 'warning'
                    tup[2] = rank
                    tup[3] = format_unix(ts)
                if rank == 0:
                    tup[0] = tup[2] = tup[3] = 'never'
                task_table.append('<tr class="%s"><td title="%s">%s<td>%s<td>%s' % tuple(tup))

            worker_table = []
            for k, st in sorted(wst.iteritems(), key=lambda i: i[1].condor_id):
                worker_table.append(
                    '<tr><td>%s<td>%s<td>%s' %
                    (st.condor_id,
                     format_unix(st.timestamp),
                     len(st.assigned.working) if st.assigned else 0))

            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()
            self.wfile.write(TEMPLATE % dict(
                hostname=socket.gethostname(),
                remaining_tasks=sum(1 for v in tst.values() if not v.is_finished()),
                all_tasks=len(tst),
                busy_workers=sum(1 for v in wst.values() if v.assigned),
                all_workers=len(wst),
                task_table=''.join(task_table),
                worker_table=''.join(worker_table),
                ))

    httpd = BaseHTTPServer.HTTPServer(('', 12345), Handler)

    t = threading.Thread(target=httpd.serve_forever)
    t.isDaemon = True
    t.start()

    logger.info('serving status information at http://localhost:12345')
