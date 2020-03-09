from flask import Flask, request
from flask_restful import Resource, Api

import argparse, os, base64
from datetime import datetime
import torch
import requests
import shutil
from data.data_loader import SpectrogramDataset, AudioDataLoader, SpectrogramParser
from decoder import GreedyDecoder
from decoder import BeamCTCDecoder
from opts import add_decoder_args, add_inference_args
from model import DeepSpeech
from utils import load_model

import pickledb

parser = argparse.ArgumentParser(description='DeepSpeech API')
parser.add_argument('--device', type=str, default='cuda')
parser.add_argument('--model', type=str, default='weights.pth')
parser.add_argument('--dict', type=str, default='dict.json')
parser.add_argument('--test', default=None)
parser.add_argument('--new-data-root-path', type=str, default='/new_data')
parser.add_argument('--db-path', type=str, default='service_db')
parser = add_decoder_args(parser)

tmp_path = '/tmp_files'
new_data_path = ''
db_path = ''
pending_db = None
commit_db = None
lock_db = None
sys_db = None
pending_count = 0
pending_buffer_count = 20

app = Flask(__name__)
api = Api(app)
torch.set_grad_enabled(False)
device = None
model = None
greedy_decoder = None
beam_decoder = None
audio_parser = None


def b64_to_file(b64_string, fpath):
    with open(fpath,'wb') as fh:
        fh.write(base64.decodebytes(b64_string.encode()))


def file2pendding(filepath):
    with open(filepath, 'r') as f:
        for ln in f:
            line = ln.rstrip('\n')
            fields = line.split(',')
            pending_db_path.set()



def analysis(file_path, decoder_type='greedy'):
    global model, device, greedy_decoder, beam_decoder, audio_parser
    input_data = audio_parser.parse_audio(file_path)
    #input = torch.zeros(1, 1, input_data.size(0), input_data.size(1))
    input = input_data.reshape(1, 1, input_data.size(0), input_data.size(1))
    input = input.to(device)
    input_sizes = torch.tensor([input.size(3)], dtype=torch.int32)
    print(input.shape)
    print(input.size(3))
    print(input_sizes)
    #input[0][0].narrow(1, 0, input_data.size(1)).copy_(input.reshape(1, 1, input_data.size(0), input_data.size(1)))
    output, output_sizes = model(input, input_sizes)
    print('[Done]')
    os.remove(file_path)
    if decoder_type == 'greedy':
        transcript, _= greedy_decoder.decode(output, output_sizes)
    else:
        transcript, _= beam_decoder.decode(output, output_sizes)
    print(transcript)
    return {'transcript': transcript}


class Speech2Text(Resource):
    def get(self, task_id):
        global tmp_path
        output = {'return_code': -1}
        if task_id == 'analysis':
            input_json = request.get_json()
            if input_json is None:
                return output
            src = input_json['source']
            data = input_json['data']
            fmt = input_json['format']
            decoder_type = 'greedy'
            if 'decoder' in input_json:
                decoder_type = input_json['decoder']
            result_json = None
            if src == 'base64':
                tmp_filename = os.path.join(tmp_path, '%s.%s'%(datetime.now().strftime('%d%m%Y%H%M%S%f'), fmt))
                b64_to_file(data, tmp_filename)
                if fmt == 'wav':
                    result_json = analysis(tmp_filename, decoder_type)
                else:
                    result_json = {'unsupported format'}
            elif src == 'http':
                tmp_filename = os.path.join(tmp_path, '%s.%s'%(datetime.now().strftime('%d%m%Y%H%M%S%f'), fmt))
                resp = requests.get(data, stream=True)
                resp.raw.decode_content = True
                with open(tmp_filename, 'wb') as imgf:
                    shutil.copyfileobj(resp.raw, imgf)
                del resp
                result_json = analysis(tmp_filename, decoder_type)
            if result_json is not None:
                output['result'] = result_json
                output['return_code'] = 0
            else:
                output['return_code'] = -1
        elif task_id == 'new_data':
            input_json = requests.get_json()
            src = input_json['source']
            data = input_json['data']
            transcript = input_json['transcript']
            output['return_code'] = 0
        elif task_id == 'review':
            input_json = requests.get_json()
            action = input_json['action']
            if action == 'list':
                start_from = 0
                end_at = 1
                if 'start-from' in input_json:
                    start_from = int(input_json['start-from'])
                if 'end-at' in input_json:
                    end_at = int(input_json['end-at'])
                output['result'] = pending_db.getall()[start_from:end_at]
                output['return_code'] = 0
            elif action == 'count':
                output['result'] = pending_db.totalkeys()
                output['return_code'] = 0
            else:
                output['message'] = 'Invalid action type for review operation.'
                output['return_code'] = -1
        else:
            output['operations'] = [
                {
                    'name': 'analysis',
                    'help': 'Given WAV file and output transcript.'
                },
                {
                    'name': 'new_data',
                    'help': 'Given WAV and corresponding transcript for reinforcement.'
                }
            ]
        return output

    def post(self, task_id):
        global tmp_path, lock_db, pending_db, new_data_path, new_data_root_path
        output = {'return_code': -1}
        if task_id == 'analysis':
            input_json = request.get_json()
            if input_json is None:
                return output
            src = input_json['source']
            data = input_json['data']
            fmt = input_json['format']
            decoder_type = 'greedy'
            if 'decoder' in input_json:
                decoder_type = input_json['decoder']
            result_json = None
            if src == 'base64':
                tmp_filename = os.path.join(tmp_path, '%s.%s'%(datetime.now().strftime('%d%m%Y%H%M%S%f'), fmt))
                b64_to_file(data, tmp_filename)
                if fmt == 'wav':
                    result_json = analysis(tmp_filename, decoder_type)
                else:
                    result_json = {'unsupported format'}
            elif src == 'http':
                tmp_filename = os.path.join(tmp_path, '%s.%s'%(datetime.now().strftime('%d%m%Y%H%M%S%f'), fmt))
                resp = requests.get(data, stream=True)
                resp.raw.decode_content = True
                with open(tmp_filename, 'wb') as imgf:
                    shutil.copyfileobj(resp.raw, imgf)
                del resp
                result_json = analysis(tmp_filename, decoder_type)
            if result_json is not None:
                output['result'] = result_json
                output['return_code'] = 0
            else:
                output['return_code'] = -1
        elif task_id == 'new_data':
            input_json = requests.get_json()
            src = input_json['source']
            data = input_json['data']
            fmt = input_json['format']
            if fmt!='wav' and fmt!='mp3':
                output['message'] = 'Invalid audio format'
                output['return_code'] = -2
            else:
                transcript = input_json['transcript']
                if transcript == '':
                    output['message'] = 'transcript cannot be empty'
                    output['return_code'] = -3
                else:
                    tmp_filename = '%s.%s' % (datetime.now().strftime('%d%m%Y%H%M%S%f'), fmt)
                    tmp_filepath = os.path.join(new_data_path, tmp_filename)
                    if src == 'http':
                        resp = requests.get(data, stream=True)
                        resp.raw.decode_content = True
                        with open(tmp_filepath, 'wb') as imgf:
                            shutil.copyfileobj(resp.raw, imgf)
                        del resp
                    elif src == 'base64':
                        b64_to_file(data, tmp_filepath)
                    else:
                        output['message'] = 'Invalid source'
                        output['return_code'] = -4
                    pending_db.set(tmp_filename, transcript)
                    pending_db.dump()
                    output['return_code'] = 0
        elif task_id == 'review':
            input_json = requests.get_json()
            action = input_json['action']
            if action == 'commit':
                data_path = input_json['data_path']
                transcript = input_json['transcript']
                owner = input_json['owner']
                current_ds = datetime.now()
                if transcript == '':
                    output['message'] = 'transcript cannot be empty'
                    output['return_code'] = -3
                elif not pending_db.exists(data_path):
                    output['message'] = 'Data does not existed.'
                    output['return_code'] = -1
                elif lock_db.exists(data_path):
                    lock_data = lock_db[data_path].split('_')
                    if lock_data[0]!=owner and current_ds < datetime.strptime(lock_data[1]):
                        output['message'] = 'The record is holding by someone. Please try again later.'
                        output['return_code'] = -2
                    else:
                        commit_db.set(data_path, transcript)
                        commit_db.dump()
                        lock_db.rem(data_path)
                        output['return_code'] = 0
                else:
                    commit_db.set(data_path, transcript)
                    commit_db.dump()
                    lock_db.rem(data_path)
                    output['return_code'] = 0
            elif action == 'remove':
                data_path = input_json['data_path']
                current_ds = datetime.now()
                owner = input_json['owner']
                if not pending_db.exists(data_path):
                    output['message'] = 'Data does not existed.'
                    output['return_code'] = -1
                elif lock_db.exists(data_path):
                    lock_data = lock_db[data_path].split('_')
                    if owner!=lock_data[0] and current_ds < datetime.strftime(lock_data[1]):
                        output['message'] = 'The record is holding by someone. Please try again later.'
                        output['return_code'] = -2
                    else:
                        pending_db.rem(data_path)
                        pending_db.dump()
                        lock_db.rem(data_path)
                        output['return_code'] = 0
                else:
                    pending_db.rem(data_path)
                    pending_db.dump()
                    lock_db.rem(data_path)
                    output['return_code'] = 0
            elif action == 'reserve':
                data_path = input_json['data_path']
                owner = input_json['owner']
                current_ds = datetime.now()
                if not pending_db.exists(data_path):
                    output['message'] = 'Data does not existed.'
                    output['return_code'] = -1
                elif lock_db.exists(data_path):
                    release_data = lock_db.get(data_path).split('_')
                    if release_data[0] == owner:
                        release_time = datetime.now() + datetime.timedelta(seconds=60).strftime('%d%m%Y%H%M%S%f')
                        lock_db.set(data_path, '%s_%s' % (release_time, owner))
                    elif current_ds < datetime.strftime(release_data[1]):
                        output['message'] = 'The record is holding by someone. Please try again later.'
                        output['return_code'] = -2
                else:
                    release_time = datetime.now() + datetime.timedelta(seconds=60).strftime('%d%m%Y%H%M%S%f')
                    lock_db.set(data_path, '%s_%s'%(release_time, owner))

            elif action == 'release':
                data_path = input_json['data_path']
                owner = input_json['owner']
                if lock_db.exists(data_path):
                    release_data = lock_db.get(data_path).split('_')
                    if release_data[0]!=owner:
                        output['message'] = 'Invalid Ownership.'
                        output['return_code'] = -1
                    else:
                        lock_db.rem(data_path)

            else:
                output['return_code'] = -1
        return output


api.add_resource(Speech2Text, '/asr/<string:task_id>')

if __name__ == '__main__':
    args = parser.parse_args()

    torch.set_grad_enabled(False)
    device = torch.device("cuda")
    #package = torch.load(args.model, map_location=lambda storage, loc: storage)
    #model = DeepSpeech.load_model_package(package).to(device)
    model = load_model(device, args.model, False)
    greedy_decoder = GreedyDecoder(model.labels, blank_index=model.labels.index('_'))
    beam_decoder = BeamCTCDecoder(model.labels, lm_path=args.lm_path, alpha=args.alpha, beta=args.beta,
                                 cutoff_top_n=args.cutoff_top_n, cutoff_prob=args.cutoff_prob,
                                 beam_width=args.beam_width, num_processes=args.lm_workers)
    audio_parser = SpectrogramParser(audio_conf=model.audio_conf, normalize=True)

    if args.test is not None:
        analysis(args.test)

    new_data_root_path = args.new_data_root_path
    db_path = args.db_path

    if not os.path.isdir(db_path):
        os.mkdir(db_path)
    pending_db_path = os.path.join(db_path, 'pending.db')
    commit_db_path = os.path.join(db_path, 'valid.db')
    lock_db_path = os.path.join(db_path, 'lock.db')
    sys_db_path = os.path.join(db_path, 'sys.db')
    pending_db = pickledb.load(pending_db_path, False)
    lock_db = pickledb.load(lock_db_path, False)
    sys_db = pickledb.load(sys_db_path, False)
    commit_db = pickledb.load(commit_db_path, False)
    if sys_db.exists('data_root_path'):
        new_data_root_path = sys_db.get('data_root_path')
        print('Root path has changed from %d to %d'%(new_data_root_path, args.new_data_root_path, new_data_root_path))
    else:
        sys_db.set('data_root_path', new_data_root_path)
    new_data_path = os.path.join(new_data_root_path, 'data')
    if not os.path.isdir(new_data_root_path):
        os.mkdir(new_data_root_path)
    if not os.path.isdir(new_data_path):
        os.mkdir(new_data_path)

    #test_dataset = SpectrogramDataset(audio_conf=model.audio_conf, manifest_filepath=args.test_manifest,
    #                                  labels=model.labels, normalize=True)
    if not os.path.isdir(tmp_path):
        os.mkdir(tmp_path)
    app.run(host='0.0.0.0', port=5000)