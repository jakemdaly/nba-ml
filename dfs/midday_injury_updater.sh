PATH=/home/jakemdaly/anaconda3/bin:/home/jakemdaly/anaconda3/condabin:/home/jakemdaly/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
eval "$(conda shell.bash hook)"
conda activate base
python midday_injury_updater.py >> /home/jakemdaly/Documents/GitRepos/nba-ml/dfs/updated_players.txt