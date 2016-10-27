import json
from urllib.parse import urlparse
from tldextract import tldextract
import os
from urllib.parse import unquote, quote
import re


regex = re.compile('(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s!()[]{};:\'".,<>?\xab\xbb\u201c\u201d\u2018\u2019]))',re.IGNORECASE)


wat_file = "..\\..\\..\\data\\sample_500.json"
#wat_file = "..\\..\\..\\data\\CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wat"

ENVELOPE = 'Envelope'
WARC_HEADER = 'WARC-Header-Metadata'
WARC_TYPE = 'WARC-Type'
WARC_TARGET_URI = 'WARC-Target-URI'
PAYLOAD = 'Payload-Metadata'
HTTP_RESPONSE = 'HTTP-Response-Metadata'
HTML_METADATA = 'HTML-Metadata'
LINKS = 'Links'

DOT = '.'
FILE_SEP = '\t'
DOMAIN = 'Domain'
PAGE = 'Page'
SOURCE = 'Source'
DESTINATION = 'Destination'
NODE = 'Node'

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
        if LINKS in json[ENVELOPE][PAYLOAD][HTTP_RESPONSE][HTML_METADATA]:
            page_links = json[ENVELOPE][PAYLOAD][HTTP_RESPONSE][HTML_METADATA][LINKS]
            print(filter(page_links))
    return filter(page_links)


JAVASCRIPT = 'javascript:'
MAIL_TO = "mailto:"
COMMA = ","

def filter(links):
    """
    Given a list of dictionaries returns a set of all valid links contained in the dictionaries
    :param links:
    :return:
    """
    results = []
    for link in links:
        add = False
        result = None
        for key, value in link.items():
            try:
                url_result = urlparse(value)
            except:
                continue
            if key in URL_TYPES and JAVASCRIPT not in value and MAIL_TO not in value and COMMA not in value and bool(url_result.scheme):
                result = value
            if HREF in value:
                add = True
        if add and result:
            results += [unquote(result).replace(" ", "%20").replace(":&#47;&#47;", "://")]
    return list(set(results))



def parse(wat_file):
    MODE = "wt"
    with open(wat_file, "r", encoding="utf-8", newline='') as inf, \
            open("..\\..\\..\\data\\nodes.csv", MODE, encoding="utf-8", newline='') as node_f, \
            open("..\\..\\..\\data\\parents_relationship.csv", MODE, encoding="utf-8", newline='') as par_f, \
            open("..\\..\\..\\data\\nodes_links.csv", MODE, encoding="utf-8", newline='') as links_f:
        par_f.write(DOMAIN + FILE_SEP + PAGE + os.linesep)
        links_f.write(SOURCE + FILE_SEP + DESTINATION + os.linesep)
        node_f.write(NODE + os.linesep)
        for line in inf:
            if "{" not in line:
                continue
            json_object = json.loads(line)
            if is_response(json_object):
                target = get_target(json_object)
                links = get_links(json_object)
                target_domain = get_domain(target)
                parent_nodes = [(target_domain, target)] + list(zip(map(get_domain, links), links))
                nodes = set()
                for parent, node in parent_nodes:
                    nodes.add(parent)
                    nodes.add(node)
                    par_f.write(parent + FILE_SEP + node + os.linesep)
                for link in links:
                    nodes.add(link)
                    links_f.write(target + FILE_SEP + link + os.linesep)
                for node in nodes:
                    node_f.write(node + os.linesep)


def get_domain(uri):
    parsed_uri = urlparse(uri)
    netloc = '{uri.netloc}'.format(uri=parsed_uri)
    extract_result = tldextract.extract(netloc)
    domain = extract_result.domain + DOT + extract_result.suffix
    return domain


def main():
    parse(wat_file)

if __name__ == "__main__":
    main()

