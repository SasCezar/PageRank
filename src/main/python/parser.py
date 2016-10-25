import json
import urlparse
from tldextract import tldextract
import os

wat_file = "..\\..\\..\\data\\sample_20.json"

ENVELOPE = 'Envelope'
WARC_HEADER = 'WARC-Header-Metadata'
WARC_TYPE = 'WARC-Type'
WARC_TARGET_URI = 'WARC-Target-URI'
PAYLOAD = 'Payload-Metadata'
HTTP_RESPONSE = 'HTTP-Response-Metadata'
HTML_METADATA = 'HTML-Metadata'
LINKS = 'Links'

DOT = '.'
FILE_SEP = ','
DOMAIN = 'Domain'
PAGE = 'Page'
SOURCE = 'Source'
DESTINATION = 'Destination'

RESPONSE = 'response'
HREF = 'href'
URL = 'url'
URL_TYPES = [HREF, URL]


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
        page_links = json[ENVELOPE][PAYLOAD][HTTP_RESPONSE][HTML_METADATA][LINKS]
    return filter(page_links)


def filter(links):
    """
    Given a list of dictionaries returns a set of all valid links contained in the dictionaries
    :param links:
    :return:
    """
    results = []
    for link in links:
        add = False
        for key, value in link.items():
            if key in URL_TYPES and bool(urlparse.urlparse(value).scheme):
                result = [value]
            if HREF in value:
                add = True
        if add:
            results += result
    return list(set(results))


def parse(wat_file):
    with open(wat_file, "r", buffering=1) as inf, open("..\\..\\..\\data\\nodes_parents_relationship.csv", "wb") as par_f, open("..\\..\\..\\data\\nodes_links.csv", "wb") as links_f:
        par_f.write(DOMAIN + FILE_SEP + PAGE + os.linesep)
        links_f.write(SOURCE + FILE_SEP + DESTINATION + os.linesep)
        for line in inf:
            if "{" not in line:
                continue
            json_object = json.loads(line)
            if is_response(json_object):
                target = get_target(json_object)
                links = get_links(json_object)
                target_domain = get_domain(target)
                parent_nodes = [(target_domain, target)] + zip(map(get_domain, links), links)
                for parent, node in parent_nodes:
                    par_f.write(parent + FILE_SEP + node + os.linesep)
                for link in links:
                 links_f.write(target_domain + FILE_SEP + link + os.linesep)


def get_domain(uri):
    parsed_uri = urlparse.urlparse(uri)
    netloc = '{uri.netloc}'.format(uri=parsed_uri)
    extract_result = tldextract.extract(netloc)
    domain = extract_result.domain + DOT + extract_result.suffix
    return domain


def main():
    parse(wat_file)

if __name__ == "__main__":
    main()

