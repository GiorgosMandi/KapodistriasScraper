import pandas as pd

from wiki_parser import kapodistrias_au_parser
from wiki_parser import french_au_parser
import argparse


def kapodistria_dataset_constructor():
    remained_units = pd.read_csv('datasets/Kapodistrias_scheme/Remained.csv', sep='\t')['Remained'].values

    # If the files exist they are loaded, else they are generated through wiki_scraper
    try:
        if args.country == 'G':
            rc = pd.read_csv("datasets/Kapodistrias_scheme/Regions_Prefectures.csv", sep='\t')
            cm = pd.read_csv("datasets/Kapodistrias_scheme/Prefectures_Municipalities.csv", sep='\t')
            md = pd.read_csv("datasets/Kapodistrias_scheme/Municipalities_Districts.csv", sep='\t')

    except FileNotFoundError:
        rc, cm, md = kapodistrias_au_parser()

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
    dataset.to_csv("datasets/Kapodistrias_scheme/Kapodistrias_AU.csv", sep='\t', index=False)

    print("Regions:\t\t", len(regions_labels))
    print("Prefectures:\t\t", len(prefectures_labels))
    print("Municipalities:\t", len(municipalities_labels))
    print("\nTotal:\t\t", len(labels))


# ----------------------------------------------------------------------------------------------------------------------


def french_dataset():
    try:
        nr = pd.read_csv("datasets/French_scheme/New_Regions.csv", sep='\t')
        fr = pd.read_csv("datasets/French_scheme/Former_Regions.csv", sep='\t')
        mm = pd.read_csv("datasets/French_scheme/Merged_Map.csv", sep='\t')
        ru = pd.read_csv("datasets/French_scheme/Remained.csv", sep='\t')
    except FileNotFoundError:
        nr, fr, mm, ru = french_au_parser()





    fr_labels = list(fr.columns)
    fr_IDs = ['0' + "%02d" % i + '00' for i in range(1, len(fr_labels) + 1)]
    fr_UpperLevel = ['<France>'] * len(fr_labels)
    fr_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                    for label in fr_labels]

    fr_departments_labels = []
    fr_departments_IDs = []
    fr_departments_URIs = []
    fr_departments_UpperLevel = []
    for index, r in enumerate(fr_labels):
        fr_departments = list(fr[r].dropna())
        fr_departments_labels += fr_departments
        fr_departments_IDs += [fr_IDs[index][:3] + "%02d" % i for i in range(1, len(fr_departments) + 1)]
        fr_departments_UpperLevel += [fr_URIs[index]] * len(fr_departments)
        fr_departments_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                                for label in fr_departments]



    nr_labels = list(nr.columns)
    nr_IDs = ['1' + "%02d" % i + '00' for i in range(1, len(nr_labels) + 1)]
    nr_UpperLevel = ['<France>'] * len(nr_labels)
    nr_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
               for label in nr_labels]

    nr_departments_labels = []
    nr_departments_IDs = []
    nr_departments_URIs = []
    nr_departments_UpperLevel = []
    for index, r in enumerate(nr_labels):
        nr_departments = list(nr[r].dropna())
        nr_departments_labels += nr_departments
        nr_departments_IDs += [nr_IDs[index][:3] + "%02d" % i for i in range(1, len(nr_departments) + 1)]
        nr_departments_UpperLevel += [nr_URIs[index]] * len(nr_departments)
        nr_departments_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                                for label in nr_departments]



    ru_labels = list(ru.columns)
    ru_IDs = ['2' + "%02d" % i + '00' for i in range(1, len(ru_labels) + 1)]
    ru_UpperLevel = ['<France>'] * len(ru_labels)
    ru_URIs = ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
               for label in ru_labels]

    ru_departments_labels = []
    ru_departments_IDs = []
    ru_departments_URIs = []
    ru_departments_UpperLevel = []
    for index, r in enumerate(ru_labels):
        ru_departments = list(ru[r].dropna())
        ru_departments_labels += ru_departments
        ru_departments_IDs += [ru_IDs[index][:3] + "%02d" % i for i in range(1, len(ru_departments) + 1)]
        ru_departments_UpperLevel += [ru_URIs[index]] * len(ru_departments)
        ru_departments_URIs += ['<https://en.wikipedia.org/wiki/' + label.replace(' ', '_') + '>'
                                for label in ru_departments]





    URIs = fr_URIs + nr_URIs + ru_URIs + fr_departments_URIs + nr_departments_URIs + ru_departments_URIs
    IDs = ['<French_AU_' + ids + '>' for ids in fr_IDs + nr_IDs + ru_IDs  + fr_departments_IDs + nr_departments_IDs \
           + ru_departments_IDs]

    labels = [label for label in fr_labels + nr_labels + ru_labels + fr_departments_labels + nr_departments_labels + \
              ru_departments_labels]

    UpperLevels = fr_UpperLevel + nr_UpperLevel + ru_UpperLevel + fr_departments_UpperLevel + nr_departments_UpperLevel \
                  + ru_departments_UpperLevel

    types = ['<Region>'] * len(fr_URIs + nr_URIs + ru_URIs) +     \
            ['<Department>'] * len(fr_departments_URIs + nr_departments_URIs + ru_departments_URIs)

    print(len(URIs), len(IDs), len(labels), len(UpperLevels), len(types))


    size = len(labels)
    former_size = len(fr_URIs)
    new_size = len(nr_URIs)
    remained_size = len(ru_URIs)


    subjects = []
    predicates = []
    objects = []
    # add Regions to the dataframe
    for i in range(former_size + new_size + remained_size):

        subjects += [URIs[i]] * 4
        predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel']
        objects += [types[i], IDs[i], '\'' + labels[i] + '\'', UpperLevels[i]]

        if i < former_size:
            subjects += [URIs[i]] * 2
            predicates += ['<wasCreatedOnDate>', '<wasDestroyedOnDate>']
            objects += ['\'1981-##-##\'^^xsd:date', '\'2016-01-01\'^^xsd:date']

        elif (i >= former_size) and (i < former_size + new_size):
            subjects += [URIs[i]]
            predicates += ['<wasCreatedOnDate>']
            objects += ['\'2016-01-01\'^^xsd:date']

        else:
            subjects += [URIs[i]]
            predicates += ['<wasCreatedOnDate>']
            objects += ['\'1981-##-##\'^^xsd:date']

    # add Departments to the dataframe
    fr_dep_size = len(fr_departments_URIs)
    nr_dep_size = len(nr_departments_URIs)
    start = former_size + new_size + remained_size
    for i in range(start, size):
        subjects += [URIs[i]] * 4
        predicates += ['rdf:type', 'monto:has_ID', 'rdf:label', 'monto:has_UpperLevel']
        objects += [types[i], IDs[i], '\'' + labels[i] + '\'', UpperLevels[i]]

        if i < fr_dep_size + start and labels.count(labels[i]) == 1:
            subjects += [URIs[i]] * 2
            predicates += ['<wasCreatedOnDate>', '<wasDestroyedOnDate>']
            objects += ['\'1981-##-##\'^^xsd:date', '\'2016-01-01\'^^xsd:date']


        elif (i >= fr_dep_size + start) and (i < nr_dep_size + fr_dep_size + start) and labels.count(labels[i]) == 1:
            subjects += [URIs[i]]
            predicates += ['<wasCreatedOnDate>']
            objects += ['\'2016-01-01\'^^xsd:date']

        else:
            subjects += [URIs[i]]
            predicates += ['<wasCreatedOnDate>']
            objects += ['\'1981-##-##\'^^xsd:date']







    # csv Construction
    dataset = pd.DataFrame({'Subject': pd.Series(subjects),
                            'Predicate': pd.Series(predicates),
                            'Object': pd.Series(objects)
                             })
    dataset.to_csv("datasets/French_scheme/French_AU.csv", sep='\t', index=False)


    # reading the file with the geometries
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f.split('.')[0] for f in listdir('datasets/French_scheme/French_Polygons')
                 if isfile(join('datasets/French_scheme/French_Polygons', f))]







parser = argparse.ArgumentParser(description='Choose country')
parser.add_argument('country', type=str, help='G for Greece, F for France', nargs='?', default='F')
args = parser.parse_args()
if args.country != 'F':
    args.country = 'G'
french_dataset()