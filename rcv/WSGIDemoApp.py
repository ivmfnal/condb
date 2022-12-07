#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from WSGIApp import WSGIApp, WSGIHandler, Request, Response, Application
#from webob import Request, Response

#
# Demo starts here
#

N = 0

class MyApp(WSGIApp):
	pass
	
class Hello(WSGIHandler):
    
	def hello(self, req, relpath, name="unknown"):
		return Response("Hello, %s" % (name,), content_type="text/plain")

class Counter(WSGIHandler):
    
	def inc(self, req, relpath, delta=1):
		global N
		N += int(delta)
		return Response("%s" % (N,), content_type="text/plain")

class TopHandler(WSGIHandler):

    def __init__(self, req, app):
		WSGIHandler.__init__(self, req, app, "")
		self.Counter = Counter(req, app)
		self.Hello = Hello(req, app)

    def env(self, req, relpath):
    
		resp = Response()
		resp.write("""
		    <html>
		    <body>
		    <pre>
		    my path = %s
		    relpath = %s
		    query = %s
		    """ % (self.MyPath, relpath, req.query_string))
		for a, v in req.GET.items():
		    resp.write('%s = %s\n' % (a,v))
		resp.write("""
		    </pre>
		<table>""")
		for k, v in req.environ.items():
			resp.write("""
			<tr><td>%s</td><td>%s</td></tr>""" % (k,v))
		resp.write("""
		    </body>
		    </html>""")
		return resp
        
app = Application(MyApp, TopHandler)