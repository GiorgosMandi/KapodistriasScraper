import pandas as pd
from wiki_parser import wiki_scraper

# dataset with the matches in yagoDateFacts
yago_matched = pd.read_csv('datasets/yago/yago_matched.tsv', sep='\t')
legistlations = ('<https://el.wikipedia.org/wiki/Σχέδιο_«Καποδίστριας»>',
                 '<https://el.wikipedia.org/wiki/Πρόγραμμα_«Καλλικράτης»>')

# insert legislations data in the output csv
temporalID = ['<1>'] * 2 + ['<2>'] * 2
subjects = [legistlations[0]] * 2 + [legistlations[1]] * 2
predicates = ['rdf:type', 'rdf:label', 'rdf:type', 'myonto:has_label']
objects = ['<LegislativeModification>', '\'Σχέδιο_«Καποδίστριας»\'',
           '<LegislativeModification>', '\'Πρόγραμμα_«Καλλικράτης»\'']

# If the files exist they are loaded, else they are generated through wiki_scraper
try:
    rc = pd.read_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t')
    cm = pd.read_csv("datasets/Kapodistrias_scheme/Prefectures_Municipalities.csv", sep='\t')

except FileNotFoundError:
    rc, cm = wiki_scraper()

# reads the entities from the dataframes and constructs/stores the neseccary values
# IDs will be Kapodistria_RRCCMM where RR is region's id, CC prefecture's id and MM municipality's id
# URIs will be url to the wikipedia page
# NOTE: URIs of municipalities doesn't reflect to the actual wiki pages

# REGIONS
regions_labels = list(rc.columns)
regions_IDs = ["%02d" % i + '0000' for i in range(1, len(regions_labels)+1)]
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
    prefectures_IDs += ["%02d" % (index + 1) + "%02d" % i + '00' for i in range(1, len(r_prefectures)+1)]
    prefectures_UpperLevel += [regions_URIs[index]] * len(r_prefectures)
    prefectures_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                         for label in r_prefectures]

# MUNICIPALITIES
municipalities_lables = []
municipalities_IDs = []
municipalities_URIs = []
municipalities_UpperLevel = []
for index, c in enumerate(prefectures_labels):
    c_municipalities = list(cm[c].dropna())
    municipalities_lables += c_municipalities
    municipalities_IDs += [prefectures_IDs[index][:-2] + "%02d" % i for i in range(1, len(c_municipalities)+1)]
    municipalities_UpperLevel += [prefectures_URIs[index]] * len(c_municipalities)
    municipalities_URIs += ['<https://el.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                            for label in c_municipalities]

# all the gathered data are fused in order to create csv's columns.
# TemporalID - Subjects - Predicate - Objects
URIs = regions_URIs + prefectures_URIs + municipalities_URIs
IDs = ['<Kapodistrias_' + ids + '>' for ids in regions_IDs + prefectures_IDs + municipalities_IDs]
temporal_id = [ids[:-1] + '_1>' for ids in IDs]
labels = ['\'' + label + '\'' for label in regions_labels + prefectures_labels + municipalities_lables]
UpperLevels = regions_UpperLevel + prefectures_UpperLevel + municipalities_UpperLevel
types = ['<geoclass_first-order_administrative_division>'] * len(regions_URIs) +     \
        ['<geoclass_second-order_administrative_division>'] * len(prefectures_URIs) +   \
        ['<geoclass_third-order_administrative_division>'] * len(municipalities_URIs)


# forms the columns of the csv
size = len(labels)
for i in range(size):
    temporalID += [temporal_id[i]] * 7
    subjects += [URIs[i]] * 7
    objects += [types[i], IDs[i], labels[i], UpperLevels[i], '\'1997-##-##\'^^xsd:date',
                '\'2011-##-##\'^^xsd:date', legistlations[0]]
    predicates += ['rdf:type', 'monto:hasKapodistria_ID', 'rdf:label',
                   'monto:has_UpperLevel', '<wasCreatedOnDate>', '<wasDestroyedOnDate>',
                   'monto:basedOn']

    # in case this division was found in yagoDateFacts we connect it using sameAs predicate
    # NOTE: I use the same URI meanig I declare an kapodistria's entity to be the same with
    # a Kallikratis's one
    if URIs[i] in yago_matched['Wiki_Kapodistria'].values:
        index = yago_matched.index[yago_matched['Wiki_Kapodistria'] == URIs[i]][0]
        temporalID += [IDs[i][:-1] + '_2>'] * 3
        subjects += [URIs[i]] * 3
        predicates += ['<wasCreatedOnDate>', 'monto:basedOn', 'rdf:sameAs']
        objects += ['\'2010-##-##\'^^xsd:date', legistlations[1],
                    yago_matched['RegionUnits_yagoDate'][index]]

# csv Construction
dataset = pd.DataFrame({'TemporalID': pd.Series(temporalID),
                        'Subject': pd.Series(subjects),
                        'Predicate': pd.Series(predicates),
                        'Object': pd.Series(objects)
                        })
dataset.to_csv("datasets/Kapodistrias_scheme/Kapodistrias_AU.csv", sep='\t', index=False)

print("Regions:\t\t", len(regions_labels))
print("Prefectures:\t\t", len(prefectures_labels))
print("Municipalities:\t", len(municipalities_lables))
print("\nTotal:\t\t", len(labels))

