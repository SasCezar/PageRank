import json
from urllib.parse import urlparse
from tldextract import tldextract
from urllib.parse import unquote
import csv


"""
Configs
"""
NUM = "ALL"
QUOTING_TYPE = csv.QUOTE_MINIMAL
FILE_SEP = '\t'

"""
File constants
"""
ENVELOPE = 'Envelope'
WARC_HEADER = 'WARC-Header-Metadata'
WARC_TYPE = 'WARC-Type'
WARC_TARGET_URI = 'WARC-Target-URI'
PAYLOAD = 'Payload-Metadata'
HTTP_RESPONSE = 'HTTP-Response-Metadata'
HTML_METADATA = 'HTML-Metadata'
LINKS = 'Links'

"""
Output file constants
"""
DOMAIN = 'Domain'
PAGE = 'Page'
SOURCE = 'Source'
DESTINATION = 'Destination'
NODE = 'Node'

"""
Domain constants
"""
DOT = '.'
RESPONSE = 'response'
HREF = 'href'
URL = 'url'
URL_TYPES = [HREF, URL]

"""
To remove
"""
JAVASCRIPT = 'javascript:'
MAIL_TO = "mailto:"
COMMA = ","
TEL = "tel:"
WHATSAPP = "whatsapp:"
BLACK_LIST = [JAVASCRIPT, MAIL_TO, COMMA, TEL, WHATSAPP]


def is_response(json):
    """
    Check if the request type of the json page is a 'response'
    :param json:
    :return:
    """
    return json[ENVELOPE][WARC_HEADER][WARC_TYPE] == RESPONSE


def get_target(json):
    """
    Returns the target page of the crawler
    :param json:
    :return:
    """
    target = json[ENVELOPE][WARC_HEADER][WARC_TARGET_URI]
    return target


def get_links(json):
    """
    Returns a list of links contained in the page
    :param json:
    :return:
    """
    page_links = []
    response = json[ENVELOPE][PAYLOAD][HTTP_RESPONSE]
    if HTML_METADATA in response:
        if LINKS in json[ENVELOPE][PAYLOAD][HTTP_RESPONSE][HTML_METADATA]:
            page_links = json[ENVELOPE][PAYLOAD][HTTP_RESPONSE][HTML_METADATA][LINKS]
    return filter(page_links)


def get_domain(uri):
    """
    Given an uri returns it's domain ("without http://")
    :param uri:
    :return:
    """
    parsed_uri = urlparse(uri)
    netloc = '{uri.netloc}'.format(uri=parsed_uri)
    extract_result = tldextract.extract(netloc)
    domain = extract_result.domain + DOT + extract_result.suffix
    return domain.strip().replace("\"\"", "")


def filter(links):
    """
    Given a list of dictionaries returns a set of all valid links contained in the dictionaries
    :param links:
    :return:
    """
    results = []
    for link in links:
        for key, value in link.items():
            if key.lower() not in URL_TYPES:
                continue

            try:
                url_result = urlparse(value)
            except ValueError:
                continue

            low_value = value.lower()
            if any(element not in low_value for element in BLACK_LIST) and bool(url_result.scheme):
                result = value
                results += [unquote(result).strip().replace(" ", "%20").replace(":&#47;&#47;", "://").replace("\"\"", "")]

    return list(set(results))


def parse(wat_file):
    mode = "w"
    with open(wat_file, "r", encoding="utf-8", newline='') as inf, \
            open("..\\..\\..\\data\\nodes_" + NUM + ".csv", mode, encoding="utf8", newline='') as node_f, \
            open("..\\..\\..\\data\\parents_relationship_" + NUM + ".csv", mode, encoding="utf8", newline='') as par_f, \
            open("..\\..\\..\\data\\nodes_links_" + NUM + ".csv", mode, encoding="utf8", newline='') as links_f:
        nodes_csv = csv.writer(node_f, delimiter=FILE_SEP, quoting=QUOTING_TYPE)
        par_csv = csv.writer(par_f, delimiter=FILE_SEP, quoting=QUOTING_TYPE)
        links_csv = csv.writer(links_f, delimiter=FILE_SEP, quoting=QUOTING_TYPE)

        nodes_csv.writerow([NODE])
        par_csv.writerow([DOMAIN, PAGE])
        links_csv.writerow([SOURCE, DESTINATION])

        for line in inf:
            try:
                json_object = json.loads(line)
            except ValueError:
                continue

            if is_response(json_object):
                target = get_target(json_object).strip()
                links = get_links(json_object)
                parent_nodes = [(get_domain(target).strip(), target)] + list(zip(map(get_domain, links), links))
                nodes = set()
                for parent, node in parent_nodes:
                    nodes.add(parent)
                    nodes.add(node)
                    par_csv.writerow([parent, node])
                for link in links:
                    nodes.add(link)
                    links_csv.writerow([target, link])
                for node in nodes:
                    nodes_csv.writerow([node])


def count(path):
    i = 0
    with open(path, "r", encoding="utf8") as f:
        for _line in f:
            i += 1
    print(i)


def clean_parents(csv_file):
    with open(csv_file, "r", encoding="utf8") as par_f, \
            open("..\\..\\..\\data\\correct_" + csv_file, "w", encoding="utf8", newline='') as par_o:
        csv_reader = csv.reader(par_f, delimiter=FILE_SEP)
        csv_writer = csv.writer(par_o, delimiter=FILE_SEP, quoting=QUOTING_TYPE)
        csv_writer.writerow([DOMAIN, PAGE])

        for domain, page in csv_reader:
            csv_writer.writerow([domain, page])


def extract_sample(wat_path, limit):
    with open(wat_path, "r", buffering=1, encoding="utf8") as inf, \
            open("..\\..\\..\\data\\sample_" + str(limit) + ".json", "w", encoding="utf8") as outf:
        counter = 0
        for line in inf:
            if "{" in line:
                counter += 1
                outf.write(line)
            if counter == limit:
                break
