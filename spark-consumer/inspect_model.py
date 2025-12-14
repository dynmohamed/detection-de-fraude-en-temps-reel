import joblib
import sys

try:
    model = joblib.load("/app/model_finale/LightGBM.pkl")
    print("Model loaded successfully")
    
    if hasattr(model, "booster_"):
        print("Feature names:", model.booster_.feature_name())
    elif hasattr(model, "feature_name_"):
        print("Feature names:", model.feature_name_)
    elif hasattr(model, "n_features_in_"):
        print("Number of features:", model.n_features_in_)
        if hasattr(model, "feature_names_in_"):
            print("Feature names in:", model.feature_names_in_)
    else:
        print("Could not determine feature names")
        print(dir(model))

except Exception as e:
    print(f"Error: {e}")
