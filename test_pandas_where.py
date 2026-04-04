import pandas as pd
import numpy as np

df = pd.DataFrame({
    'int_col': [1, 2],
    'float_col': [1.1, np.nan],
    'str_col': ['a', None]
})

df2 = df.where(pd.notna(df), None)
print("Normal where:")
print(df2.to_dict(orient='records'))

df3 = df.astype(object).where(pd.notna(df), None)
print("Object where:")
print(df3.to_dict(orient='records'))

df4 = df.replace({np.nan: None})
print("Replace where:")
print(df4.to_dict(orient='records'))

