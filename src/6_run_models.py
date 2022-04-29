import torch
import torch.nn as nn

class MLP():
    def __init__(self, layer_sizes, lr=0.001):
        if len(layer_sizes) < 2: raise ValueError("Two layers are required.")
        self.layer_sizes = layer_sizes
        self.layers = []
        for i in range(len(self.layer_sizes)-1):
            self.layers.append(nn.Linear(self.layer_sizes[i], self.layer_sizes[i+1]))
            self.layers.append(nn.ReLU())
        self.model = nn.Sequential(*self.layers)
        self.loss_function = nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)

class Dataset(torch.utils.data.Dataset):
    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data[idx].asDict()
        y_keys = ('box_office', 'audience_score', 'critics_score', 'averageRating')
        x = torch.FloatTensor(tuple(float(row[k]) for k in row if k not in y_keys))
        y = torch.FloatTensor(tuple(float(row[k]) for k in y_keys))
        return x, y

mlp = MLP((37, 37, 37, 37, 4))
print(mlp.model)
train_set = Dataset(dataset)
dataloader = torch.utils.data.DataLoader(train_set, batch_size=32, shuffle=True)

def train(model, epoches, dataloader):
    i = 0
    for epoch in range(epoches):
        for x, y in dataloader:
          model.optimizer.zero_grad()
          outputs = model.model(x)
          loss = model.loss_function(outputs, y)
          loss.backward()
          model.optimizer.step()
          i+=1
          if i % 1000 == 0:
              print(loss.item())

train(mlp, 100, dataloader)
