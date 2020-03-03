from flask import Flask
from flask_restful import Resource, Api

import argparse
import json
import os
import random
import time

import numpy as np
import torch.distributed as dist
import torch.utils.data.distributed
from apex import amp
from apex.parallel import DistributedDataParallel
from warpctc_pytorch import CTCLoss

from data.data_loader import AudioDataLoader, SpectrogramDataset, BucketingSampler, DistributedBucketingSampler
from decoder import GreedyDecoder
from logger import VisdomLogger, TensorBoardLogger
from model import DeepSpeech, supported_rnns
from test import evaluate
from utils import reduce_tensor, check_loss
from opts import add_decoder_args

app = Flask(__name__)
api = Api(app)


class Speech2Text(Resource):
    def get(self):
        return {'ret_code': 0}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)