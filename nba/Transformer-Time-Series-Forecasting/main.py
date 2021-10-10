import argparse
# from train_teacher_forcing import *
from train_with_sampling import *
from DataLoader import *
from torch.utils.data import DataLoader
import torch.nn as nn
import torch
from helpers import *
from inference import *

def main(
    epoch: int = 1,
    k: int = 8,
    batch_size: int = 1,
    frequency: int = 100,
    training_length = 82, # minimum num of games a player must have to be used a train data
    forecast_window = 30,
    home = "nba/Transformer-Time-Series-Forecasting/",
    path_to_save_model = "save_model/",
    path_to_save_loss = "save_loss/", 
    path_to_save_predictions = "save_predictions/", 
    device = "cpu"
):

    clean_directory()

    data = [doc for doc in nba_db.DatasetTransformer.find({}) if len(doc['data'])>training_length+forecast_window]
    random.shuffle(data)
    size = len(data)
    train = data[:int(size*.85)]
    test = data[int(size*.85):]
    del data

    # train_dataset = SensorDataset(csv_name = train_csv, root_dir = home + "Data/", training_length = training_length, forecast_window = forecast_window)
    # train_dataloader = DataLoader(train_dataset, batch_size=1, shuffle=True)
    # test_dataset = SensorDataset(csv_name = test_csv, root_dir = home + "Data/", training_length = training_length, forecast_window = forecast_window)
    # test_dataloader = DataLoader(test_dataset, batch_size=1, shuffle=True)
    train_dataset = GameLogDataset(training_length, forecast_window, train)
    del train
    train_dataloader = DataLoader(train_dataset, batch_size=1, shuffle=True)
    test_dataset = GameLogDataset(training_length, forecast_window, test)
    del test
    test_dataloader = DataLoader(test_dataset, batch_size=1, shuffle=True)

    best_model = transformer(train_dataloader, epoch, k, frequency, path_to_save_model, path_to_save_loss, path_to_save_predictions, device, forecast_window)
    inference(path_to_save_predictions, forecast_window, test_dataloader, device, path_to_save_model, best_model)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--epoch", type=int, default=1)
    parser.add_argument("--k", type=int, default=60)
    parser.add_argument("--batch_size", type=int, default=1)
    parser.add_argument("--frequency", type=int, default=100)
    parser.add_argument("--path_to_save_model",type=str,default="save_model/")
    parser.add_argument("--path_to_save_loss",type=str,default="save_loss/")
    parser.add_argument("--path_to_save_predictions",type=str,default="save_predictions/")
    parser.add_argument("--device", type=str, default="cpu")
    args = parser.parse_args()

    main(
        epoch=args.epoch,
        k = args.k,
        batch_size=args.batch_size,
        frequency=args.frequency,
        path_to_save_model=args.path_to_save_model,
        path_to_save_loss=args.path_to_save_loss,
        path_to_save_predictions=args.path_to_save_predictions,
        device=args.device,
    )
