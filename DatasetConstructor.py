import pandas as pd
from wiki_parser import kapodistrias_au_parser
from wiki_parser import french_au_parser


def kapodistria_dataset_constructor(rc, cm, md, path='datasets/Kapodistrias_scheme/'):
    remained_units = pd.read_csv(path + 'Remained.csv', sep='\t')['Remained'].values

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
    subjects = []
    predicates = []
    objects = []
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
    dataset.to_csv(path + "Kapodistrias_AU.csv", sep='\t', index=False)

    return dataset


# ----------------------------------------------------------------------------------------------------------------------


def french_dataset_constructor(nr, fr, ru, path='datasets/French_scheme/'):

    # Forming data for the future regions
    fr_labels = list(fr.columns)
    fr_IDs = ['<French_AU_0' + "%02d" % i + '00>'  for i in range(1, len(fr_labels) + 1)]
    fr_UpperLevels = ['<France>'] * len(fr_labels)
    fr_types = ['Region'] * len(fr_labels)
    fr_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                    for label in fr_labels]
    # Forming the data for the former Departments
    for index, r in enumerate(list(fr.columns)):
        fr_departments = list(fr[r].dropna())
        fr_labels += fr_departments
        fr_IDs += [fr_IDs[index][:-3] + "%02d>" % i for i in range(1, len(fr_departments) + 1)]
        fr_UpperLevels += [fr_URIs[index]] * len(fr_departments)
        fr_types += ['Department'] * len(fr_departments)
        fr_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                                for label in fr_departments]


    # Forming the data for the new Regions
    nr_labels = list(nr.columns)
    nr_IDs = ['<French_AU_1' + "%02d" % i + '00>' for i in range(1, len(nr_labels) + 1)]
    nr_UpperLevels = ['<France>'] * len(nr_labels)
    nr_types = ['Region'] * len(nr_labels)
    nr_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
               for label in nr_labels]
    # Forming the data for the new Departments
    for index, r in enumerate(list(nr.columns)):
        nr_departments = list(nr[r].dropna())
        nr_labels += nr_departments
        nr_IDs += [nr_IDs[index][:-3] + "%02d>" % i for i in range(1, len(nr_departments) + 1)]
        nr_UpperLevels += [nr_URIs[index]] * len(nr_departments)
        nr_types += ['Department'] * len(nr_departments)
        nr_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                    for label in nr_departments]


    # Forming the data of the regions that remained the same
    ru_labels = list(ru.columns)
    ru_IDs = ['<French_AU_2' + "%02d" % i + '00>' for i in range(1, len(ru_labels) + 1)]
    ru_UpperLevels = ['<France>'] * len(ru_labels)
    ru_types = ['Region'] * len(ru_labels)
    ru_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
               for label in ru_labels]
    # Forming the data for the Departments of regions that didn't change
    for index, r in enumerate(list(ru.columns)):
        ru_departments = list(ru[r].dropna())
        ru_labels += ru_departments
        ru_IDs += [ru_IDs[index][:-3] + "%02d>" % i for i in range(1, len(ru_departments) + 1)]
        ru_UpperLevels += [ru_URIs[index]] * len(ru_departments)
        ru_types += ['Department'] * len(ru_departments)
        ru_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                                for label in ru_departments]

    # former regions dataset
    fr_subjects = []
    fr_predicates = []
    fr_objects = []
    for i in range(len(fr_URIs)):
        fr_subjects += [fr_URIs[i]] * 6
        fr_predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel',
                       '<wasCreatedOnDate>', '<wasDestroyedOnDate>']
        fr_objects += [fr_types[i], fr_IDs[i], '\'' + fr_labels[i] + '\'', fr_UpperLevels[i],
                    '\'1981-##-##\'^^xsd:date', '\'2016-01-01\'^^xsd:date']

    # new regions dataset
    nr_subjects = []
    nr_predicates = []
    nr_objects = []
    for i in range(len(nr_URIs)):
        nr_subjects += [nr_URIs[i]] * 5
        nr_predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel',
                       '<wasCreatedOnDate>', ]
        nr_objects += [nr_types[i], nr_IDs[i], '\'' + nr_labels[i] + '\'', nr_UpperLevels[i],
                    '\'2016-01-01\'^^xsd:date']

    # add remained regions to both datasets
    for i in range(len(ru_URIs)):
        fr_subjects += [ru_URIs[i]] * 5
        nr_subjects += [ru_URIs[i]] * 5

        fr_predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel',
                       '<wasCreatedOnDate>']
        nr_predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel',
                       '<wasCreatedOnDate>']
        fr_objects += [ru_types[i], ru_IDs[i], '\'' + ru_labels[i] + '\'', ru_UpperLevels[i],
                    '\'1981-##-##\'^^xsd:date']
        nr_objects += [ru_types[i], ru_IDs[i], '\'' + ru_labels[i] + '\'', ru_UpperLevels[i],
                    '\'1981-##-##\'^^xsd:date']

    # Construction of the DataFrames and csv
    # Former Regions
    fr_dataset = pd.DataFrame({'Subject': pd.Series(fr_subjects),
                            'Predicate': pd.Series(fr_predicates),
                            'Object': pd.Series(fr_objects)})
    fr_dataset.to_csv(path + "French_FAU.csv", sep='\t', index=False)
    # New Regions
    nr_dataset = pd.DataFrame({'Subject': pd.Series(nr_subjects),
                            'Predicate': pd.Series(nr_predicates),
                            'Object': pd.Series(nr_objects)})
    nr_dataset.to_csv(path + "French_NAU.csv", sep='\t', index=False)


    return nr_dataset, fr_dataset

    # reading the file with the geometries
    #from os import listdir
    #from os.path import isfile, join
    #onlyfiles = [f.split('.')[0] for f in listdir('datasets/French_scheme/French_Polygons')
    #             if isfile(join('datasets/French_scheme/French_Polygons', f))]
