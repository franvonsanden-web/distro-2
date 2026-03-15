import requests
import re
import sys

r = requests.get('https://www.disco.com.uy', headers={'User-Agent': 'Mozilla/5.0'})
print('Status:', r.status_code)

matches = re.findall(r'https://[^"''\s]+/api/[^"''\s]+', r.text)
print('API links:', set(matches))

graphql = re.findall(r'https://[^"''\s]+/graphql[^"''\s]*', r.text)
print('GraphQL links:', set(graphql))

print("Done.")
