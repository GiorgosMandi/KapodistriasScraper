import pandas as pd
from wiki_parser import wiki_scraper

import argparse

parser = argparse.ArgumentParser(description='Choose country')
parser.add_argument('country', type=str, help='G for Greece, F for France', nargs='?', default='G')
args = parser.parse_args()
if args.country != 'F':
    args.country = 'G'


remained_units = pd.read_csv('datasets/Remained.csv', sep='\t')['Remained'].values
subjects = []
predicates = []
objects = []

# If the files exist they are loaded, else they are generated through wiki_scraper
try:
    if args.country == 'G':
        rc = pd.read_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t')
        cm = pd.read_csv("datasets/Kapodistrias_scheme/Prefectures_Municipalities.csv", sep='\t')
        md = pd.read_csv("datasets/Kapodistrias_scheme/Municipalities_Districts.csv", sep='\t')

except FileNotFoundError:
    rc, cm, md = wiki_scraper()

# reads the entities from the dataframes and constructs/stores the neseccary values
# IDs will be Kapodistria_RRCCMM where RR is region's id, CC prefecture's id and MM municipality's id
# URIs will be url to the wikipedia page
# NOTE: URIs of municipalities doesn't reflect to the actual wiki pages

# REGIONS
regions_labels = list(rc.columns)
regions_IDs = ["%02d" % i + '000000' for i in range(1, len(regions_labels)+1)]
regions_UpperLevel = ['<Greece>'] * len(regions_labels)
regions_URIs = ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                for label in regions_labels]

# PREFECTURES
prefectures_labels = []
prefectures_IDs = []
prefectures_URIs = []
prefectures_UpperLevel = []
for index, r in enumerate(regions_labels):
    r_prefectures = list(rc[r].dropna())
    prefectures_labels += r_prefectures
    prefectures_IDs += ["%02d" % (index + 1) + "%02d" % i + '0000' for i in range(1, len(r_prefectures)+1)]
    prefectures_UpperLevel += [regions_URIs[index]] * len(r_prefectures)
    prefectures_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                         for label in r_prefectures]

# MUNICIPALITIES
municipalities_labels = []
municipalities_IDs = []
municipalities_URIs = []
municipalities_UpperLevel = []
for index, c in enumerate(prefectures_labels):
    c_municipalities = list(cm[c].dropna())
    municipalities_labels += c_municipalities
    municipalities_IDs += [prefectures_IDs[index][:-4] + "%02d" % i + '00' for i in range(1, len(c_municipalities)+1)]
    municipalities_UpperLevel += [prefectures_URIs[index]] * len(c_municipalities)
    municipalities_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                            for label in c_municipalities]

# DISTRICTS
districts_labels = []
districts_IDs = []
districts_URIs = []
districts_UpperLevel = []
for index, m in enumerate(municipalities_labels):
    m_districts = list(md[m].dropna())
    districts_labels += m_districts
    districts_IDs += [municipalities_IDs[index][:-2] + "%02d" % i for i in range(1, len(m_districts)+1)]
    districts_UpperLevel += [municipalities_URIs[index]] * len(m_districts)
    districts_URIs += ['<Kapodistrias_district_' + label.replace(' ', '_') + '>'
                       for label in m_districts]


# all the gathered data are fused in order to create csv's columns.
# TemporalID - Subjects - Predicate - Objects
URIs = regions_URIs + prefectures_URIs + municipalities_URIs + districts_URIs
IDs = ['<Kapodistrias_' + ids + '>' for ids in regions_IDs + prefectures_IDs + municipalities_IDs + districts_IDs]
labels = [label for label in regions_labels + prefectures_labels + municipalities_labels + districts_labels]
UpperLevels = regions_UpperLevel + prefectures_UpperLevel + municipalities_UpperLevel + districts_UpperLevel
types = ['<Region>'] * len(regions_URIs) +     \
        ['<Prefecture>'] * len(prefectures_URIs) +   \
        ['<Municipality>'] * len(municipalities_URIs) +\
        ['<District>'] * len(districts_URIs)


# forms the columns of the csv
size = len(labels)
for i in range(size):
    subjects += [URIs[i]] * 5
    objects += [types[i], IDs[i], '\'' + labels[i] + '\'', UpperLevels[i], '\'1997-##-##\'^^xsd:date']
    predicates += ['rdf:type', 'monto:hasKapodistrias_ID', 'rdf:label', 'monto:has_UpperLevel',
                   '<wasCreatedOnDate>']
    # inserts destruction date if it is not contained in remained_units
    if labels[i] not in remained_units:
        subjects += [URIs[i]]
        objects += ['\'2011-##-##\'^^xsd:date']
        predicates += ['<wasDestroyedOnDate>']

# csv Construction
dataset = pd.DataFrame({'Subject': pd.Series(subjects),
                        'Predicate': pd.Series(predicates),
                        'Object': pd.Series(objects)
                        })
dataset.to_csv("datasets/Kapodistrias_scheme/Kapodistrias_AU.csv", sep='\t', index=False)

print("Regions:\t\t", len(regions_labels))
print("Prefectures:\t\t", len(prefectures_labels))
print("Municipalities:\t", len(municipalities_labels))
print("\nTotal:\t\t", len(labels))

