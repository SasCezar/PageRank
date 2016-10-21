
wat_file = "CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wat"
limit = 500

with open(wat_file, "r", buffering=1) as inf, open("sample_" + str(limit) + ".json", "w") as outf:
	tag_counter = 0
	counter = 0
	for line in inf:
		if "{" in line:	
			counter += 1
		outf.write(line)
		if counter == limit:
			break