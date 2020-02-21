import os, sys, re, argparse, csv, ntpath

TMP_DIR = 'tmp'

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


def add_2_dict(target_file, words, end_str=','):
    fs = None
    if not os.path.isfile(target_file):
        fs = open(target_file, 'w')
        fs.write('[\n')
    else:
        fs = open(target_file, 'a+')
    for word in words:
        fs.write('\"')
        fs.write(word)
        if end_str is not None:
            fs.write('\"%s\n'%end_str)
        else:
            fs.write('\"\n')


def close_dict(target_file):
    with open(target_file, 'a+') as fs:
        fs.write(']')


def buffer_dict_continue(target_file):
    global TMP_DIR
    if not os.path.isdir(TMP_DIR):
        os.mkdir(TMP_DIR)
    if not os.path.isfile(target_file):
        return False
    else:
        tmp_path = os.path.join(TMP_DIR, 'tmp_dict.txt')
        with open(target_file, 'r') as inputf:
            with open(tmp_path, 'w') as outputf:
                for ln in inputf:
                    line = ln.rstrip('\n')
                    outputf.write(line)
                    outputf.write('\n')
                    if not line.endswith('[') and not line.endswith(','):
                        break
        return True

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
                        outf.write(line)





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