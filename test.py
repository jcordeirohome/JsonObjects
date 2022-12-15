import json
from JsonObjects import jDocument

# Opening JSON file
f = open('squad.json')
data = json.load(f)
f.close()

# create the jDocument
jDoc = jDocument(data)

# get some values from the document
print(f"project.name = { jDoc['project.name'] }")
print(f"members[1] = { jDoc['members[1]'] }")
print(f"members[2] = { jDoc['members[2]'] }")
print(f"members[name=Eternal Flame] = { jDoc['members[name=Eternal Flame]'] }")
