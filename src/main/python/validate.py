import csv

with open("..\\..\\..\\data\\correct_correct_nodes_links.csv", "r", encoding="utf8") as par_f, \
        open("..\\..\\..\\data\\x_correct_correct_nodes_links.csv", "w", encoding="utf8", newline='') as par_o:
    csv_reader = csv.reader(par_f, delimiter='\t')
    csv_writer = csv.writer(par_o, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    for page in csv_reader:
        if len(page) == 2:
            for a,b in zip(page[0::2], page[1::2]):
                if "http" not in a or "http" not in b:
                    print(page)
                else:
                    csv_writer.writerow(page)
        else:
            pass
        #    print(page)

