Random developer notes:

Logging:
I think we're following this guy's advice:
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/


# Standalone Workers

Our goal is to be able to spin up N workers of an instance type, have
them be able to pull tasks off the queue, run them with our
environment, etc.

Use SQS for jobs. SQS queue is set in the config file, used at setup. 
Each machine runs a daemon that pulls from SQS queue, runs things, returns
results. 
Each worker checks its uptime and the queue status and self-terminates
after a certain amount of time. 

Todo: 
[ ] Refactor wrenhandler to be more platform-agnostic. 
[ ] refactor Wren to let us invoke via non-Lambda mechanisms


