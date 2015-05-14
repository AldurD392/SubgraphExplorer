import os
import sys

len_dict = dict()
content_dict = dict()
for dirpath, _, file_list in os.walk(sys.argv[1]):
    for f in file_list:
        if f.startswith(".") or f == "_FOUND":
            continue

        with open(os.path.join(dirpath, f)) as input_file:
            content = input_file.readline().rstrip()
            l = content.split(" ")
            len_dict[f] = len(l)
            content_dict[f] = content


m = min(len_dict, key=len_dict.get)
print("{} - {} - {}".format(m, len_dict[m], content_dict[m]))
