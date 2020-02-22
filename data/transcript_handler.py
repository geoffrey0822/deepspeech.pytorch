import os, sys, re, argparse, csv, ntpath, shutil
from opencc import OpenCC
import json

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
        fs = open(target_file, 'w', encoding='utf8')
        fs.write('[\n')
    else:
        fs = open(target_file, 'a+', encoding='utf8')
    for word in words:
        fs.write('\"')
        fs.write(word)
        if end_str is not None:
            fs.write('\"%s\n'%end_str)
        else:
            fs.write('\"\n')
    fs.close()


def close_dict(target_file, remove_last=False):
    if remove_last:
        f = open(target_file, 'rb+')
        f.seek(-3, os.SEEK_END)
        f.truncate()
        f.close()
    with open(target_file, 'a+', encoding='utf8') as fs:
        fs.write('\n]')


def buffer_dict_continue(target_file):
    global TMP_DIR
    if not os.path.isdir(TMP_DIR):
        os.mkdir(TMP_DIR)
    if not os.path.isfile(target_file):
        return False
    else:
        tmp_path = os.path.join(TMP_DIR, 'tmp_dict.txt')
        with open(target_file, 'r', encoding='utf8') as inputf:
            with open(tmp_path, 'w', encoding='utf8') as outputf:
                for ln in inputf:
                    line = ln.rstrip('\n')
                    outputf.write(line)
                    outputf.write('\n')
                    if not line.endswith('[') and not line.endswith(','):
                        break
        shutil.copyfile(tmp_path, target_file)
        return True


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def remove_duplicates_dict(dict_file):
    ds = json.load(open(dict_file, 'r', encoding='utf8'))
    unique_char = {char for char in ds}
    with open(dict_file, 'w', encoding='utf8') as outf:
        #print(unique_char)
        json_str = json.dumps(list(unique_char), ensure_ascii=False)
        outf.write(json_str)


def reformat_dict(dict_file):
    ds = json.load(open(dict_file, 'r', encoding='utf8'))
    unique_char = list({char for char in ds})
    with open(dict_file, 'w', encoding='utf8') as outf:
        outf.write('[\n')
        last_char = unique_char[-1]
        for char in unique_char:
            outf.write('\"')
            outf.write(char)
            outf.write('\"')
            if char != last_char:
                outf.write(',\n')
            else:
                outf.write('\n')
        outf.write(']')


def process_files(rec_file, dst, new_rec_file, dict_file, simplified=False,
                  remove_path=None, replace_path=None):
    buffer_dict_continue(dict_file)
    if not os.path.isdir(dst):
        os.mkdir(dst)
    rec_f = None
    cc = None
    if simplified:
        cc = OpenCC('s2hk')
    if os.path.isfile(new_rec_file):
        rec_f = open(new_rec_file, 'a+')
    else:
        rec_f = open(new_rec_file, 'w')
    total = 0
    with open(rec_file, 'r') as f:
        for ln in f:
            total+=1
    with open(rec_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        count = 0
        for row in reader:
            audio_file = row[0].replace('\\','/')
            txt_file = row[1].replace('\\','/')
            if remove_path is not None:
                #print(audio_file)
                audio_file = audio_file.replace(remove_path.replace('\\','/'),replace_path)
                txt_file = txt_file.replace(remove_path.replace('\\','/'),replace_path)
                #print(audio_file)
                #audio_file = os.path.join(replace_path, audio_file)
                #txt_file = os.path.join(replace_path,os.path.relpath(txt_file, remove_path.replace('\\','/')))
            new_file_path = os.path.join(dst, path_leaf(txt_file))
            with open(new_file_path, 'w', encoding='utf8') as outf:
                with open(txt_file, 'r', encoding='utf8') as txtf:
                    for ln in txtf:
                        line = ln.rstrip('\n')
                        if cc is not None:
                            line = cc.convert(line)
                        line = line.replace(' ','').replace('(','').replace(')','').replace('_','').replace('~')
                        cchar = seg_char(line)
                        outf.write(line)
                        add_2_dict(dict_file, cchar)
            rec_f.write('%s,%s\n'%(audio_file, new_file_path))
            count+=1
            if count%1000==0:
                print('processed %d/%d data'%(count, total))
    rec_f.close()
    close_dict(dict_file, True)
    remove_duplicates_dict(dict_file)
    reformat_dict(dict_file)


def main():
    parser = argparse.ArgumentParser(description='Corpus Transformation')
    parser.add_argument('--dst', type=str, help='Exportation path of transformed result')
    parser.add_argument('--data-file', type=str, help='Data record file')
    parser.add_argument('--dict', type=str, help='Dictionary output')
    parser.add_argument('--simplified', type=int, default=0, help='Is it simplified chinese?')
    parser.add_argument('--output-file', type=str, help='New data record file')
    parser.add_argument('--remove-path', default=None, help='Invalid Root Path to be removed')
    parser.add_argument('--replace-path', default=None, help='Root path to be replaced with remove-path')
    args = parser.parse_args()

    process_files(args.data_file, args.dst, args.output_file, args.dict, args.simplified == 1,
                  args.remove_path, args.replace_path)


if __name__ == "__main__":
    main()