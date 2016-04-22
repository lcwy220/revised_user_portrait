#!/bin/sh
cd /home/ubuntu01/user_portrait/user_portrait/cron/flow2
tmux kill-session -t flow2
tmux new-session -s flow2 -d
tmux new-window -n first -t flow2
tmux new-window -n redis -t flow2
#python stop_zmq_vent.py >> /home/log/flow2.log
python del_file_yes.py >> /home/log/flow2.log
tmux send-keys -t flow2:redis 'python zmq_vent_weibo_flow2.py' C-m
python restart_zmq_vent.py >> /home/log/flow2.log
