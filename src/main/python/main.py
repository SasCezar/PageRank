import wat_parser

samples = [15000]
wat_file = "..\\..\\..\\data\\CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wat"

def main():
    for sample in samples:
        wat_parser.extract_sample(wat_file, sample)
        wat_parser.NUM = str(sample)
        wat_parser.parse("..\\..\\..\\data\\sample_" + str(sample) + ".json")


if __name__ == "__main__":
    main()
