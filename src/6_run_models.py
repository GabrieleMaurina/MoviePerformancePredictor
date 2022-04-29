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

def train(model, epoches, dataloader):
    k = 1000
    losses = []
    average_loss = 1.0
    for epoch in range(epoches):
        for x, y in dataloader:
          model.optimizer.zero_grad()
          outputs = model.model(x)
          loss = model.loss_function(outputs, y)
          loss.backward()
          model.optimizer.step()
          losses.append(loss.item())
          if len(losses) == k:
              avg = sum(losses)/k
              print(avg)
              #if avg > average_loss: return
              losses = []
              average_loss = avg

mlp = MLP((37, 37, 4), lr=0.001)
print(mlp.model)
train_set = Dataset(dataset)
dataloader = torch.utils.data.DataLoader(train_set, batch_size=50, shuffle=True)
train(mlp, 100, dataloader)
