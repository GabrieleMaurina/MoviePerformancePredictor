import os
import sys
import torch
import torch.nn as nn
import torch.distributed as dist
import pandas as pd

NODES = ('beaverhead', 'stillwater', 'fitzgerald')
PORT = 89111

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
                  if avg > average_loss: return
                  losses = []
                  average_loss = avg

    def average_gradients():
        world_size = dist.get_world_size()
        for param in self.model.parameters():
            dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
            param.grad.data /= world_size

class Dataset(torch.utils.data.Dataset):
    def __init__(self, path, y_keys, drop_keys):
        if isinstance(y_keys, str): y_keys = [y_keys,]
        else: y_keys = list(y_keys)
        if isinstance(drop_keys, str): drop_keys = [drop_keys,]
        else: drop_keys = list(drop_keys)
        self.y_keys = y_keys
        self.drop_keys = drop_keys

        self.x = pd.read_csv(path, sep='\t')
        self.x.drop(drop_keys, axis=1)
        self.y = self.x[y_keys]
        self.x.drop(y_keys, axis=1)

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        pass
        #x = torch.FloatTensor(tuple(float(row[k]) for k in row if k not in self.y_keys))
        #y = torch.FloatTensor(tuple(float(row[k]) for k in self.y_keys))
        #return x, y

def train_validate():
    model = DistributedMLP((89, 89, 4))
    model.train()

def main():
    Dataset('data/normalized_data.tsv', ('box_office', 'audience_score', 'critics_score', 'averageRating'), ('tconst', 'primaryTitle'))

    return
    if len(sys.argv) == 1:
        pass
    else:
        rank, world_size, master, port = sys.argv[1:5]
        os.environ['MASTER_ADDR'] = master
        os.environ['MASTER_PORT'] = port
        dist.init_process_group('gloo', rank=rank, world_size=world_size)
        run(rank, world_size)
        dist.destroy_process_group()

if __name__ == '__main__':
    main()
