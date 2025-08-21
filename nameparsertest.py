import re
from nameparser import HumanName

def process_names():
    with open(r'C:\Users\jgfri\OneDrive\Desktop\ml-playground\entities\person.txt', 'r',encoding='utf-8') as f:
        names = []
        for line in f:
            name = line.strip()
            if not re.match(r'^[a-zA-Z\s\-\.]+$', name):
                continue
            parsed = HumanName(name)
            if not parsed.first or not parsed.last:
                continue
            if len(parsed.first) < 3 or len(parsed.last) < 3:
                continue
            names.append(f"{parsed.first.title()} {parsed.last.title()}")
        return set(names)
    

print(process_names())