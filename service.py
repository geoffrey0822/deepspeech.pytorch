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
from utils import load_model

parser = argparse.ArgumentParser(description='DeepSpeech API')
parser.add_argument('--device', type=str, default='cuda')
parser.add_argument('--model', type=str, default='weights.pth')
parser.add_argument('--dict', type=str, default='dict.json')
parser.add_argument('--test', default=None)
parser = add_decoder_args(parser)

tmp_path = '/tmp_files'

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


def analysis(file_path, decoder_type='greedy'):
    global model, device, greedy_decoder, beam_decoder, audio_parser
    with torch.no_grad():
        input_data = audio_parser.parse_audio(file_path)
        #input = torch.zeros(1, 1, input_data.size(0), input_data.size(1))
        input = input_data.reshape(1, 1, input_data.size(0), input_data.size(1))
        input = input.to(device)
        input_sizes = torch.IntTensor(input.size(3))
        print(input.shape)
        print(input.size(3))
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
        return output


api.add_resource(Speech2Text, '/asr/<string:task_id>')

if __name__ == '__main__':
    args = parser.parse_args()
    device = torch.device("cuda")
    model = load_model(device, args.model, False)
    model.eval()
    greedy_decoder = GreedyDecoder(model.labels, blank_index=model.labels.index('_'))
    beam_decoder = BeamCTCDecoder(model.labels, lm_path=args.lm_path, alpha=args.alpha, beta=args.beta,
                                 cutoff_top_n=args.cutoff_top_n, cutoff_prob=args.cutoff_prob,
                                 beam_width=args.beam_width, num_processes=args.lm_workers)
    audio_parser = SpectrogramParser(audio_conf=model.audio_conf, normalize=True)

    if args.test is not None:
        analysis(args.test)
    #test_dataset = SpectrogramDataset(audio_conf=model.audio_conf, manifest_filepath=args.test_manifest,
    #                                  labels=model.labels, normalize=True)
    if not os.path.isdir(tmp_path):
        os.mkdir(tmp_path)
    app.run(host='0.0.0.0', port=5000)