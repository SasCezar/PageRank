import csv

with open("..\\..\\..\\data\\parents_relationship_ALL.csv", "r", encoding="utf8") as par_f, \
        open("..\\..\\..\\data\\correct_parents_relationship_ALL.csv", "w", encoding="utf8", newline='') as par_o:
    csv_reader = csv.reader(par_f, delimiter='\t')
    csv_writer = csv.writer(par_o, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["Source", "Destination"])
    for page in csv_reader:
        if len(page) == 2 or '' in page:
            csv_writer.writerow(page)
            """
            for a, b in zip(page[0::2], page[1::2]):
                if "http" not in a or "http" not in b:
                    print(page)
                else:
                    csv_writer.writerow(page)
            """
        else:
            print(page)
