import os
import sys
import torch
import torch.nn as nn
import torch.distributed as dist
import pandas as pd
import subprocess
from multiprocessing import Pool
from time import time

NODES = ('beaverhead', 'stillwater', 'fitzgerald')
PORT = 29522
DATASET = 'data/normalized_data.tsv'

class DistributedMLP():
    def __init__(self, layer_sizes, learning_rate, momentum):
        if len(layer_sizes) < 2: raise ValueError("Two layers are required.")
        self.layer_sizes = layer_sizes
        self.layers = []
        for i in range(len(self.layer_sizes)-1):
            self.layers.append(nn.Linear(self.layer_sizes[i], self.layer_sizes[i+1]))
            self.layers.append(nn.ReLU())
        self.model = nn.Sequential(*self.layers)
        self.loss_function = nn.L1Loss()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=learning_rate, momentum=momentum)

    def train(self, data, epoches, bach_size):
        dataloader = torch.utils.data.DataLoader(data, batch_size=bach_size)
        k = 2000
        losses = []
        average_loss = 1.0
        for epoch in range(epoches):
            for x, y in dataloader:
                self.optimizer.zero_grad()
                loss = self.loss_function(self.model(x), y)
                loss.backward()
                self.average_gradients()
                self.optimizer.step()
                losses.append(loss.item())
                if len(losses) == k:
                    avg = sum(losses)/k
                    print(avg)
                    if avg > average_loss: return
                    losses = []
                    average_loss = avg

    def average_gradients(self):
        world_size = dist.get_world_size()
        for param in self.model.parameters():
            dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
            param.grad.data /= world_size

class Dataset(torch.utils.data.Dataset):
    def __init__(self, df, y_keys, drop_keys):
        if isinstance(y_keys, str): y_keys = [y_keys,]
        else: y_keys = list(y_keys)
        if isinstance(drop_keys, str): drop_keys = [drop_keys,]
        else: drop_keys = list(drop_keys)
        self.y_keys = y_keys
        self.drop_keys = drop_keys

        self.x = df.drop(drop_keys, axis=1)
        self.y = self.x[y_keys]
        self.x.drop(y_keys, axis=1)

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        x = torch.FloatTensor(tuple(float(v) for v in self.x.iloc[idx]))
        y = torch.FloatTensor(tuple(float(v) for v in self.y.iloc[idx]))
        return x, y

def full_dataset(data):
    return tuple(torch.stack(v) for v in zip(*data))

def train_validate():
    model = DistributedMLP((93, 93, 4), 0.001, 0.5)
    train_dataset, validate_dataset = load_dataset(DATASET, split=0.9)
    model.train(train_dataset, 10, 50)
    validation_x, validation_y = full_dataset(validate_dataset)
    print(model.loss_function(model.model(validation_x), validation_y).item())

def load_dataset(path, split=0.9):
    df = pd.read_csv(path, sep='\t')
    split_size = int(len(df) * split)
    y_keys = ('box_office', 'audience_score', 'critics_score', 'averageRating')
    drop_keys = ('tconst', 'primaryTitle')
    return Dataset(df[:split_size], y_keys, drop_keys), Dataset(df[split_size:], y_keys, drop_keys)
    

def run_trainer_node(address):
    rank = NODES.index(address)
    world_size = len(NODES)
    master = NODES[0]
    port = PORT
    args = f'{rank} {world_size} {master} {port}'
    process = subprocess.run(f'ssh {address} "cd workspace/cs535/team_project ; python3.8 src/distributed_training.py {args}"', shell=True, capture_output=True, text=True)
    return tuple(float(v) for v in process.stdout.split('\n') if v)

def main():
    if len(sys.argv) == 1:
        print(f'Starting {len(NODES)} nodes: {", ".join(NODES)}')
        t = time()
        with Pool(len(NODES)) as pool:
            results = tuple(pool.map(run_trainer_node, NODES))
            for i, res in enumerate(results):
                print(NODES[i])
                for v in res:
                    print(f'\t{v:.4f}')
        print(f'Tot training time: {int(time()-t)}s')
        print(f'Validation MAE: {results[0][-1]}')
    else:
        rank, world_size, master, port = sys.argv[1:5]
        rank = int(rank)
        world_size = int(world_size)
        os.environ['MASTER_ADDR'] = master
        os.environ['MASTER_PORT'] = port
        dist.init_process_group('gloo', rank=rank, world_size=world_size)
        train_validate()
        dist.destroy_process_group()

if __name__ == '__main__':
    main()
