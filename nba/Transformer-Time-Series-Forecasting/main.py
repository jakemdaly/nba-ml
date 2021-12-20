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
    epoch: int = 10,
    k: int = 1, # with k=1, will take about 10 epochs to converge
    batch_size: int = 1,
    frequency: int = 100,
    training_length = 82, # minimum num of games a player must have to be used a train data
    forecast_window = 60,
    home = "nba/Transformer-Time-Series-Forecasting/",
    path_to_save_model = "save_model/",
    path_to_save_loss = "save_loss/", 
    path_to_save_predictions = "save_predictions/", 
    device = "cpu"
):

    clean_directory()

    data = [doc for doc in nba_db.DatasetTransformer.find({}) if len(doc['data'])>training_length+forecast_window]
    random.shuffle(data)
    # size = len(data)
    # train = data[:int(size*.85)]
    # test = data[int(size*.85):]
    # del data

    train_dataset = GameLogDataset(training_length, forecast_window, data)
    # train_dataloader = DataLoader(train_dataset, batch_size=1, shuffle=True)
    # dataset_train_eval = DataLoader(train_dataset, batch_size=1, shuffle=True)
    best_model = transformer(train_dataset, epoch, k, frequency, path_to_save_model, path_to_save_loss, path_to_save_predictions, device, forecast_window)

    train_dataset = GameLogDataset(training_length, forecast_window, data)
    dataset_train_eval = DataLoader(train_dataset, batch_size=1, shuffle=True)

    inference(path_to_save_predictions, forecast_window, dataset_train_eval, device, path_to_save_model, best_model)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--epoch", type=int, default=2)
    parser.add_argument("--k", type=int, default=1)
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

