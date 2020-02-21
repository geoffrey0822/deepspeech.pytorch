import os, sys, re, argparse, csv, ntpath


def seg_char(sent):
    # handle english first
    pattern_char_1 = re.compile(r'([\W])')
    parts = pattern_char_1.split(sent)
    parts = [p for p in parts if len(p.strip())>0]

    # handle chinese
    pattern = re.compile(r'([\u4e00-\u9fa5])')
    chars = pattern.split(sent)
    chars = [w for w in chars if len(w.strip())>0]
    return chars


def add_2_dict(target_file, words):
    fs = None
    if not os.path.isfile(target_file):
        fs = open(target_file, 'w')
        fs.write('[\n')
    else:
        fs = open(target_file, 'a+')


def closeDict(target_file):
    with open(target_file, 'a+') as fs:
        fs.write(']')


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def process_files(rec_file, dst, dict_file, simplified=False):
    if not os.path.isdir(dst):
        os.mkdir(dst)
    with open(rec_file, 'r') as f:
        reader = csv.reader(rec_file, delimiter=',')
        for row in reader:
            audio_file = row[0]
            txt_file = row[1]
            new_file_path = os.path.join(dst, path_leaf(txt_file))
            with open(new_file_path, 'w') as outf:
                with open(txt_file, 'r') as txtf:
                    for ln in txtf:
                        line = ln.rstrip('\n')
                        cchar = seg_char(line)
                        outf.write()




def main():
    parser = argparse.ArgumentParser(description='Corpus Transformation')
    parser.add_argument('--dst', type=str, help='Exportation path of transformed result')
    parser.add_argument('--data-file', type=str, help='Data record file')
    parser.add_argument('--dict', type=str, help='Dictionary output')
    parser.add_argument('--simplified', type=int, default=0, help='Is it simplified chinese?')
    parser.add_argument('--dict-append', type=int, default=0, help='0-write mode, 1-append mode')
    parser.add_argument('--output-file', type=str, help='New data record file')
    args = parser.parse_args()


if __name__ == "__main__":
    main()