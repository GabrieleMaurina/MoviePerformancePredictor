from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import torch
import torch.nn as nn
import random

class MLP():
    def __init__(self, layer_sizes, learning_rate):
        if len(layer_sizes) < 2: raise ValueError("Two layers are required.")
        self.layer_sizes = layer_sizes
        self.layers = []
        for i in range(len(self.layer_sizes)-1):
            self.layers.append(nn.Linear(self.layer_sizes[i], self.layer_sizes[i+1]))
            self.layers.append(nn.ReLU())
        self.model = nn.Sequential(*self.layers)
        self.loss_function = nn.L1Loss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)

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
              self.optimizer.step()
              losses.append(loss.item())
              if len(losses) == k:
                  avg = sum(losses)/k
                  if avg > average_loss: return
                  losses = []
                  average_loss = avg

class Dataset(torch.utils.data.Dataset):
    def __init__(self, data, y_keys):
        self.data = data
        if isinstance(y_keys, str): y_keys = (y_keys,)
        self.y_keys = y_keys

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data[idx].asDict()
        x = torch.FloatTensor(tuple(float(row[k]) for k in row if k not in self.y_keys))
        y = torch.FloatTensor(tuple(float(row[k]) for k in self.y_keys))
        return x, y

def split_datasets(data, y_keys, training_size, shuffle):
    training_size = int(len(data) * training_size)
    if shuffle:
        random.shuffle(data)
    return Dataset(data[:training_size], y_keys), Dataset(data[training_size:], y_keys)

def full_dataset(data):
    return tuple(torch.stack(v) for v in zip(*data))

def train_validate(layers, dataset, learning_rate=0.001, epoches=100, batch_size=50, y_keys=('box_office', 'audience_score', 'critics_score', 'averageRating'), training_size=0.9, shuffle=False):
    model = MLP(layers, learning_rate)
    training_data, validation_data = split_datasets(dataset, y_keys, training_size, shuffle)
    validation_x, validation_y = full_dataset(validation_data)
    model.train(training_data, epoches, batch_size)
    return model.loss_function(model.model(validation_x), validation_y).item()

def main():
    sc = SparkContext('local', '6_run_models')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/normalized_data.tsv', sep=r'\t', header=True)
    dataset = normalized_data.drop('tconst', 'primaryTitle').collect()

    size = len(dataset[0])
    s1 = size - 4
    s2 = size - 1
    print('all', train_validate((s1, s1, 4), dataset))
    print('audience_score', train_validate((s2, s2, 1), dataset, learning_rate=0.0001, y_keys='audience_score'))
    print('averageRating', train_validate((s2, s2, 1), dataset, learning_rate=0.0001, y_keys='averageRating'))
    print('box_office', train_validate((s2, s2, 1), dataset, learning_rate=0.0001, y_keys='box_office'))
    print('critics_score', train_validate((s2, s2, 1), dataset, learning_rate=0.0001, y_keys='critics_score'))

if __name__ == '__main__':
    main()