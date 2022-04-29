from scipy.stats import pearsonr
from sklearn import linear_model
import pandas as pd
import seaborn as sn
import matplotlib.pyplot as plt

# Pearson coefficient
def corr_vars(var1, var2):
  corr, _ = pearsonr(var1, var2)
  return corr

# Correlation matrix
def corr_mat(ps_df):
  df = pd.DataFrame(ps_df)
  corr_matrix = df.corr()
  sn.heatmap(corr_matrix, annot=True)
  plt.show()
  return corr_matrix

# Add data file in [dataset_nm]
#df = [dataset_nm] 

for col1 in df.columns:
  for col2 in df.columns[col1+1:]:
    corr_vars(col1, col2)
    
corr_mat(df)
