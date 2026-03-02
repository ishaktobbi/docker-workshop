import sys 
import pandas as pd

month = sys.argv[1]

df = pd.DataFrame({
    "day" : [1, 2, 3],
    "passangers" : [20, 30, 40]
})

df["month"] = int(sys.argv[1])

df.to_parquet(f"output_{month}.parquet")

print(df.head()) 