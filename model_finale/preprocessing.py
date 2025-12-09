# model/preprocessing.py

import pandas as pd
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
import joblib
import psycopg2
conn=psycopg2.connect(
    dbname="frauddb",
    user="myuser",
    password="mypassword",
    host="localhost",
    port="5432"
)
def build_preprocessing_pipeline(df):
    
    categorical = ["type"]
    numerical = [col for col in df.columns if col not in ["isfraud", "isflaggedfraud", "type"]]

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
            ("num", StandardScaler(), numerical)
        ]
    )

    return preprocess


if __name__ == "__main__":
    df=pd.read_sql("select * from transactions_raw",conn)
    df = df.drop(['nameorig', 'namedest'], axis=1)
    preprocess = build_preprocessing_pipeline(df)
    X = df.drop(['isfraud', 'isflaggedfraud'], axis=1)
    preprocess.fit(X)

    
    joblib.dump(preprocess, "model_finale/preprocess.pkl")

    print("Preprocessing pipeline saved in model/preprocess.pkl")
