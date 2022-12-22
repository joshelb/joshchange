import http.client

conn = http.client.HTTPSConnection("")

headers = { 'authorization': "Bearer accesstoken" }

conn.request("GET", "/dev-q7xsxw5kc72jd045.eu.auth0.com/api/v2/users", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))
