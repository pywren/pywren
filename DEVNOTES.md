Random developer notes:

# Logging

I think we're following this guy's advice:
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

When logging and storing metadata, try and differentiate between:

host (things that occur on user computer) and other
time (how long something took, sec) and timestamp (unix timestamp) 

## Local pip install
If you'd like to work on pywren, I recommend doing a local editable pip install
from the pywren souce dir via

```
pip install --editable ./
```


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




Supervisord notes:
# get this init script

https://gist.github.com/hilios/b4974ad4b7771571705e6d0830c67119


[program:testprogram]
command=/usr/bin/python /home/ec2-user/testprogram.py
autorestart=true


stand-alone server
1. grabs from queue, processes job out-of-band
2. if there are no jobs for 5 min and you've been idle for 5 min, terminate


order of cloud-init directives:
http://stackoverflow.com/questions/34095839/cloud-init-what-is-the-execution-order-of-cloud-config-directives

We're going to store as many of our logging results in cloudwatch as possible
our loggroup will always be pywren-standalone
and the stream will be instanceid-$FOO


http://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/EC2NewInstanceCWL.html


## How to release with PyPi:
I am mostly following http://peterdowns.com/posts/first-time-with-pypi.html . I had
been using twine but it looks like, with the latest versions of python, 
twine is unnecessary. 

Make sure you create a `~/.pypirc` file. 

First register
```
python setup.py register -r pypitest
```

Test on pypi-test
```
python setup.py sdist upload -r pypitest
```



note that travis has support for auto-releasing this, which we should investigate

## Watchtower / cloudatch
Currently we want to pass a log stream prefix to cloudwatch

http://stackoverflow.com/questions/32688688/how-to-write-setup-py-to-include-a-git-repo-as-a-dependency


## Testing python3

I (@ericmjonas) am mostly a python2 developer still, so this is how
you can set up a python3 environment inside conda to try things out.

```
conda create -n py35-pywren python=3.5 anaconda
```
and then
```
source activate py35-pywren
pip install -e pywren
```

## The runtime
The runtime is a tremendous challenge

## How to release:

We use a release versioning scheme of 0.1rc0 for dev releases and 0.1
for final releases.

1. Tag the release. If you are an animal, you can do this via `git tag
   v0.2` followed by `git push origin --tags` should work. Via magit
   you can do this via the tags popup. Push the tags to github via `P`
   `t` from the `magit status buffer`

2. Make sure everything is passing on travis

3. push to pypitest
First make sure you are setup
```
python setup.py register -r pypitest
```

```
python setup.py sdist upload -r pypitest
```
4. Test the pypitest build by updating the tag `pypitest-build` to the current build

5. Kickoff the travis build for this version by deleting and reupdating the tag. I know
this is a bit of a hack, but it works. 

```
# delete the remote tag
git push -f origin :refs/tags/pypitest-build
# delete local 
git tag -d pypitest-build
# create tag
git tag pypitest-build
# push
git push origin --tags
```

4. Create a github release via the gui

5. push to pypi

```
python setup.py register -r pypi
python setup.py sdist upload -r pypi
```

