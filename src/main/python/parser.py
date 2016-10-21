import json

wat_file = "..\\..\\..\\data\\sample_20.json"

ENVELOPE = 'Envelope'
WARC_HEADER = 'WARC-Header-Metadata'
WARC_TYPE = 'WARC-Type'
WARC_TARGET_URI = 'WARC-Target-URI'
PAYLOAD = 'Payload-Metadata'
HTTP_RESPONSE = 'HTTP-Response-Metadata'
HTML_METADATA = 'HTML-Metadata'
LINKS = 'Links'

RESPONSE = 'response'


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
    # TODO Implement
    return links


def parse(wat_file):
    with open(wat_file, "r", buffering=1) as inf, open("..\\..\\..\\data\\neo4j.csv", "w") as outf:
        for line in inf:
            if "{" not in line:
                continue
            json_object = json.loads(line)
            if is_response(json_object):
                target = get_target(json_object)
                links = get_links(json_object)
                outf.write(target + "\t" + str(links) + "\n")


def main():
    parse(wat_file)

if __name__ == "__main__":
    main()