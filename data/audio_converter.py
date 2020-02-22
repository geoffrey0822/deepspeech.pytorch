import os, sys, argparse


def main():
    parser = argparse.ArgumentParser(description='Audio Converter')
    parser.add_argument('--src', type=str, help='Path for audio files')
    parser.add_argument('--buffer-path', type=str, help='Path for backup storage')
    parser.add_argument('--sample-rate', type=int, default=16000)
    parser.add_argument('--mono', type=int, default=1)
    args = parser.parse_args()

    src = args.src
    tmp_path = args.buffer_path
    sample_rate = args.sample_rate
    isMono = True
    if args.mono == 0:
        isMono = False

    if not os.path.isdir(tmp_path):
        os.mkdir(tmp_path)

    total = 0
    for file in os.listdir(src):
        total+=1

    print('Done')


if __name__ == "__main__":
    main()