import pandas as pd
from wiki_parser import wiki_scraper

import random
seed = random.randint(1000, 9999)

# If the files exist they are loaded, else they are generated through wiki_scraper
try:
    rc = pd.read_csv("datasets/Kapodistria_scheme/Regions_Counties.csv", sep='\t')
    cm = pd.read_csv("datasets/Kapodistria_scheme/Counties_Municipalities.csv", sep='\t')

except FileNotFoundError:
    rc, cm = wiki_scraper()

# reads the entities from the dataframes and constructs ids
# Ids form will be Kapodistria_RRCCMM where RR is region's id, CC county's id and MM municipality's id
regions = list(rc.columns)
regions_id = ["%02d" % i + '0000' for i in range(1, len(regions)+1)]

counties = []
counties_id = []
for index, r in enumerate(regions):
    r_counties = list(rc[r].dropna())
    counties += r_counties
    counties_id += ["%02d" % index + "%02d" % i + '00' for i in range(1, len(r_counties)+1)]

municipalities = []
municipalities_id = []
for index, c in enumerate(counties):
    c_municipalities = list(cm[c].dropna())
    municipalities += c_municipalities
    municipalities_id += [counties_id[index][:-2] + "%02d" % i for i in range(1, len(c_municipalities)+1)]

# generates csv -- it will contain IDs Predicate Label
total_entities = regions + counties + municipalities
ids = ['Kapodistria_' + id for id in regions_id + counties_id + municipalities_id]
id_predicate = ['hasKapodistria_ID'] * len(ids)
df_total_entities = pd.DataFrame({'ID': pd.Series(ids),
                                  'ID_Predicate': pd.Series(id_predicate),
                                  'Administrative Unit': pd.Series(total_entities)
                                  })
df_total_entities.to_csv("datasets/Kapodistria_scheme/Kapodistria_AU.csv", sep='\t', index=False)

print("Regions:\t\t", len(regions))
print("Counties:\t\t", len(counties))
print("Municipalities:\t", len(municipalities))
print("\nTotal:\t\t", len(total_entities))

