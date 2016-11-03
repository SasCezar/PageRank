import wat_parser

samples = [50]
wat_file = "..\\..\\..\\data\\CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wat"

def main():
    for sample in samples:
        wat_parser.extract_sample(wat_file, sample)
        wat_parser.NUM = str(sample)
        wat_parser.parse("..\\..\\..\\data\\sample_" + str(sample) + ".json")
        wat_parser.clean_parents("..\\..\\..\\data\\parents_relationship_" + str(sample) + ".csv")


if __name__ == "__main__":
    main()