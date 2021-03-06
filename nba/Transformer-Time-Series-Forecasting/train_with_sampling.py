from model import Transformer
from torch.utils.data import DataLoader
import torch
import torch.nn as nn
from DataLoader import SensorDataset
import logging
import time # debugging
from plot import *
from helpers import *
from joblib import load
from icecream import ic
from torch.optim.lr_scheduler import ReduceLROnPlateau
import math, random

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="[%Y-%m-%d %H:%M:%S]")
logger = logging.getLogger(__name__)

def flip_from_probability(p):
    return True if random.random() < p else False

def transformer(dataloader, EPOCH, k, frequency, path_to_save_model, path_to_save_loss, path_to_save_predictions, device, forecast_size=24):

    device = torch.device(device)

    model = Transformer().double().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1)
    # scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.1, patience=200)
    criterion = torch.nn.MSELoss()
    best_model = ""
    min_train_loss = float('inf')

    for epoch in range(EPOCH + 1):
        train_loss = 0
        val_loss = 0

        ## TRAIN -- TEACHER FORCING
        model.train()
        ii=0
        for index_in, index_tar, _input, target, player in dataloader.balanced_sample():
            print(f"{player:<25}:", end='')
            # Shape of _input : [batch, input_length, feature]
            # Desired input for model: [input_length, batch, feature]

            optimizer.zero_grad()
            src = _input.permute(1,0,2).double().to(device)[:-1,:,:] # torch.Size([24, 1, 7])
            target = _input.permute(1,0,2).double().to(device)[1:,:,:] # src shifted by 1.
            sampled_src = src[:1, :, :] #t0 torch.Size([1, 1, 35])

            for i in range(len(target)-1):

                prediction = model(sampled_src, device) # torch.Size([1xw, 1, 1])
 
                """
                # to update model at every step
                # loss = criterion(prediction, target[:i+1,:,:1])
                # loss.backward()
                # optimizer.step()
                """

                if i < forecast_size: # One day, enough data to make inferences about cycles
                    prob_true_val = True
                else:
                    ## coin flip
                    v = k/(k+math.exp(epoch/k))*2 # probability of heads/tails depends on the epoch, evolves with time.
                    prob_true_val = flip_from_probability(v) # starts with over 95 % probability of true val for each flip in epoch 0.
                    ## if using true value as new value

                if prob_true_val: # Using true value as next value
                    sampled_src = torch.cat((sampled_src.detach(), src[i+1, :, :].unsqueeze(0).detach()))
                else: ## using prediction as new value
                    positional_encodings_new_val = src[i+1,:,1:].unsqueeze(0)
                    predicted_fpoints = torch.cat((prediction[-1,:,:].unsqueeze(0), positional_encodings_new_val), dim=2)
                    sampled_src = torch.cat((sampled_src.detach(), predicted_fpoints.detach()))
            
            """To update model after each sequence"""
            loss = criterion(target[:-1,:,0].unsqueeze(-1), prediction) 
            loss.backward()
            optimizer.step()
            train_loss += loss.detach().item()
            ii+=1
            print(f"{loss:.2f} \t\t Ave: {train_loss/ii:.2f}")


        if train_loss < min_train_loss:
            torch.save(model.state_dict(), path_to_save_model + f"best_train_{epoch}.pth")
            torch.save(optimizer.state_dict(), path_to_save_model + f"optimizer_{epoch}.pth")
            min_train_loss = train_loss
            best_model = f"best_train_{epoch}.pth"


        # if epoch % 10 == 0: # Plot 1-Step Predictions

        #     logger.info(f"Epoch: {epoch}, Training loss: {train_loss}")
        #     # scaler = load('scalar_item.joblib')
        #     sampled_src_fpts = sampled_src[:,:,0].cpu() #torch.Size([35, 1, 7])
        #     src_fpts = src[:,:,0].cpu() #torch.Size([35, 1, 7])
        #     target_fpts = target[:,:,0].cpu() #torch.Size([35, 1, 7])
        #     prediction_fpts = prediction[:,:,0].detach().cpu().numpy() #torch.Size([35, 1, 7])
        #     plot_training_3(epoch, path_to_save_predictions, src_fpts, sampled_src_fpts, prediction_fpts, player, index_in, index_tar)

        train_loss /= len(dataloader)
        log_loss(train_loss, path_to_save_loss, train=True)
        
    plot_loss(path_to_save_loss, train=True)
    return best_model