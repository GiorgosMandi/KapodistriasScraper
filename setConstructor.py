import pandas as pd
from wiki_parser import wiki_scraper

try:
    rc = pd.read_csv("Kapodistria_scheme/Regions_Counties.csv", sep='\t')
    cm = pd.read_csv("Kapodistria_scheme/Counties_Municipalities.csv", sep='\t')

except FileNotFoundError:
    rc, cm = wiki_scraper()

regions = list(rc.columns)
counties = []
municipalities = []
for r in regions:
    counties += list(rc[r].dropna())
for c in counties:
    municipalities += list(cm[c].dropna())
municipalities = list(set(municipalities))

total_entities = regions + counties + municipalities
count = list(range(len(total_entities)))
df_total_entities = pd.DataFrame({'id': pd.Series(count), 'Administrative Unit': pd.Series(total_entities)})
df_total_entities.to_csv("Kapodistria_scheme/Kapodistria_AU.csv", sep='\t', index=False)

print("Regions:\t\t", len(regions))
print("Counties:\t\t", len(counties))
print("Municipalities:\t", len(municipalities))
print("\nTotal:\t\t", len(total_entities))


