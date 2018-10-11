import pandas as pd
from wiki_parser import wiki_scraper


# If the files exist they are loaded, else they are generated through wiki_scraper
try:
    rc = pd.read_csv("datasets/Kapodistria_scheme/Regions_Counties.csv", sep='\t')
    cm = pd.read_csv("datasets/Kapodistria_scheme/Counties_Municipalities.csv", sep='\t')

except FileNotFoundError:
    rc, cm = wiki_scraper()

# reads the entities from the dataframes and constructs/stores the neseccary values
# IDs will be Kapodistria_RRCCMM where RR is region's id, CC county's id and MM municipality's id
# URIs will be url to the wikipedia page
# NOTE: URIs of municipalities doesn't reflect to the actual wiki pages

regions_labels = list(rc.columns)
regions_IDs = ["%02d" % i + '0000' for i in range(1, len(regions_labels)+1)]
regions_URIs = ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>' for label in regions_labels]
regions_UpperLevel = ['<Greece>'] * len(regions_labels)

counties_labels = []
counties_IDs = []
counties_URIs = []
counties_UpperLevel = []
for index, r in enumerate(regions_labels):
    r_counties = list(rc[r].dropna())
    counties_labels += r_counties
    counties_IDs += ["%02d" % index + "%02d" % i + '00' for i in range(1, len(r_counties)+1)]
    counties_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>' for label in r_counties]
    counties_UpperLevel += [regions_URIs[index]] * len(r_counties)


municipalities_lables = []
municipalities_IDs = []
municipalities_URIs = []
municipalities_UpperLevel = []
for index, c in enumerate(counties_labels):
    c_municipalities = list(cm[c].dropna())
    municipalities_lables += c_municipalities
    municipalities_IDs += [counties_IDs[index][:-2] + "%02d" % i for i in range(1, len(c_municipalities)+1)]
    municipalities_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                            for label in c_municipalities]
    municipalities_UpperLevel += [counties_URIs[index]] * len(c_municipalities)

# all the gathered data are fused in order to create csv's columns.
# Subjects & Objects
labels = ['\'' + label + '\'' for label in regions_labels + counties_labels + municipalities_lables]
URIs = regions_URIs + counties_URIs + municipalities_URIs
IDs = ['Kapodistria_' + id for id in regions_IDs + counties_IDs + municipalities_IDs]
UpperLevels = regions_UpperLevel + counties_UpperLevel + municipalities_UpperLevel
types = ['<geoclass_first-order_administrative_division>'] * len(regions_URIs) +     \
        ['<geoclass_second-order_administrative_division>'] * len(counties_URIs) +   \
        ['<geoclass_third-order_administrative_division>'] * len(municipalities_URIs)

# forms the columns of the csv
size = len(labels)
subjects = []
predicates = []
objects = []
for i in range(size):
    subjects += [URIs[i]] * 4
    predicates += ['rdf:type', 'monto:hasKapodistria_ID', 'myonto:has_label', 'monto:has_UpperLevel']
    objects += [types[i], IDs[i], labels[i], UpperLevels[i]]

# csv Construction
dataset = pd.DataFrame({'Subject': pd.Series(subjects),
                        'Predicate': pd.Series(predicates),
                        'Object': pd.Series(objects)
                        })
dataset.to_csv("datasets/Kapodistria_scheme/Kapodistria_AU.csv", sep='\t', index=False)

print("Regions:\t\t", len(regions_labels))
print("Counties:\t\t", len(counties_labels))
print("Municipalities:\t", len(municipalities_lables))
print("\nTotal:\t\t", len(labels))

