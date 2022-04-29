# Written by Eric Kearney
import re

TITLES = 'data/title_basics.tsv'
CREW = 'data/title_crew.tsv'
NAMES = 'data/name_basics.tsv'

OUTPUT = 'data/merged_title_crew.tsv'

def open_and_process_file(file):
    movies = {}
    i = 0
    with open(file) as f:
        values = f.readlines()[1:]
        for value in values:
            i += 1
            columns = re.split(r'\t+', value)
            const = columns[0]
            other_values = columns[1:]
            movies[const] = other_values

    return movies


def convert_nconst_to_name(names, crew):
    for _, crew_members in crew.items():
        for i, crew_member in enumerate(crew_members):
            if crew_member in names:
                crew_members[i] = names[crew_member][0]


def merge_dictionaries(dict1, dict2):
    merged = dict1.copy()
    for key, value in dict2.items():
        if key in merged:
            merged[key].extend(value)

    return merged


def write_to_tsv(titles_crew):
    with open(OUTPUT, 'w') as f:
        f.write('tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\tdirectors\twriters\n')
        for key, value in titles_crew.items():
            line = key
            for v in value:
                line += ('\t' + v.replace('\n', ''))
            line += '\n'
            f.write(line)


def main():
    titles = open_and_process_file(TITLES)
    crew = open_and_process_file(CREW)
    names = open_and_process_file(NAMES)

    convert_nconst_to_name(names, crew)

    titles_crew = merge_dictionaries(titles, crew)

    write_to_tsv(titles_crew)



if __name__ == '__main__':
    main()
