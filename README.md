# torqueSchedule -- EVAWIZ schedule for torque PBS
Schedule program designed for PBS system with gpu enabled. It is run and tested on PBS 4.2.9.
## Usage

```
eva get torqueSchedule
evaload torqueSchedule
cd ~/evawiz/torqueSchedule/
mv perm.config.example perm.config #revise its content as you needed
scheduled start
```

scheduled need to run with an argument to spicefy wihch operation to be executed.
```
ot@mu01 torqueSchedule]# ./scheduled -h
Usage: scheduled [operation]
run
  run the schedule program
start
  start the schedule daemon
stop
  stop the schedule daemon
restart
  restart the schedule daemon
status
  checking the running status of the shedule daemon
help
  show this help information
```

## specify queue setting perm.comfig
You should follow the pattern in the perm.config.


