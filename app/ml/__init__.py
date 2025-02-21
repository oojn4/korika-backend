# ML module initialization
from app.ml.model import MultivariateTimeSeriesLSTM
from app.ml.utils import predict_six_months_ahead, generate_prediction_plots, train_or_load_model