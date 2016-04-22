#!/bin/sh
cd /home/ubuntu01/user_portrait/user_portrait/cron/flow4
tmux kill-session -t flow4
tmux new-session -s flow4 -d
tmux new-window -n first -t flow4
tmux new-window -n redis -t flow4
#python stop_zmq_vent.py >> /home/log/flow2.log
python del_file_yes.py >> /home/log/flow4.log
tmux send-keys -t flow4:redis 'python zmq_vent_weibo_flow4.py' C-m
python restart_zmq_vent.py >> /home/log/flow4.log
