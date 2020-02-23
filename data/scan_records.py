import os, sys, argparse, utils, subprocess


def regenerate(src, dst, manifest, new_manifest, sample_rate,
               mono, min_duration, max_duration):
    if not os.path.isdir(dst):
        os.mkdir(dst)

    file_paths = [os.path.join(src, filename) for filename in os.listdir(src)]
    total = len(file_paths)

    print('There are %d audio files at origin'%total)
    count = 0
    file_paths = utils.order_and_prune_files(file_paths, min_duration, max_duration)
    final_total = len(file_paths)

    with open(new_manifest, 'w') as outf:
        with open(manifest, 'r') as inputf:
            for ln in inputf:
                line = ln.rstrip('\n')
                fields = line.split(',')
                audio_path = fields[0]
                file_name = os.path.basename(audio_path)
                corpus_path = fields[1]
                if audio_path in file_paths:
                    new_audio_path = os.path.join(dst, file_name)
                    cmd = "sox {} -r {} -b 16 -c 1 {}".format(
                        audio_path,
                        sample_rate,
                        new_audio_path)
                    subprocess.call([cmd], shell=True)
                    outf.write('%s,%s\n'%(new_audio_path, corpus_path))
                    count+=1
                    if count%1000:
                        print('processed %d/%d'%(count,final_total))
    print('%d are valid audio files'%final_total)

def main():
    parser = argparse.ArgumentParser(description='Filter and conversion')
    parser.add_argument('--src', type=str, help='Path for audio files')
    parser.add_argument('--manifest', type=str, help='Manifest file for processing')
    parser.add_argument('--dst', type=str, help='Processed output path')
    parser.add_argument('--new-manifest', type=str, help='Regenerated manifest file')
    parser.add_argument('--sample-rate', type=int, default=16000)
    parser.add_argument('--mono', type=int, default=0)
    parser.add_argument('--min-duration', type=int, default=0, help='in second')
    parser.add_argument('--max-duration', type=int, default=0, help='in second')
    args = parser.parse_args()

    src = args.src
    dst = args.dst
    data_path = args.manifest
    new_data_path = args.manifest
    sample_rate = args.sample_rate
    min_duration = None
    max_duration = None
    if args.min_duration>0:
        min_duration = args.min_duration
    if args.max_duration>0:
        max_duration = args.max_duration
    isMono = False
    if args.mono == 1:
        isMono = True

    regenerate(src, dst, manifest=data_path, new_manifest=new_data_path,
               sample_rate=sample_rate, mono=isMono,
               min_duration=min_duration, max_duration=max_duration)

    print('Done')


if __name__ == "__main__":
    main()