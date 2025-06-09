#!/bin/bash

# A shell script that kills and restarts a Python download script every 3 days.

# Retrieve the script PID
PID=$(pgrep -f dataserver_download)

# Kill the script
kill $PID

# cd into the project directory
cd /home/ec2-user/var/www/starchasers-dataserver

# Source the Python virtual environment
source env/bin/activate

# Clear the existing nohup log
rm nohup.out

# Restart the script
nohup python -m bin.dataserver_download &

# Deactivate the virtual environment
deactivate

echo "Download script restarted successfully"
