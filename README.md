# PyWren

> The wrens are mostly small, brownish passerine birds in the mainly New World family Troglodytidae. ... Most wrens are small and rather inconspicuous, except for their loud and often complex songs. - Wikipedia

PyWren -- it's like a mini condor, in the cloud, for often-complex calls. 

Goal: Dispatch of small efficient tasks with as close to zero user overhead
as possible. In particular, entirely serverless -- use as many AWS services
as necessary. 

```python
def foo(b):
    x = np.random.normal(0, b, 1024)
    A = np.random.normal(0, b, (1024, 124))
    return np.dot(A, x)

res = pywren.map(foo, np.linspace(0.1, 100, 1000)
```


## Key technologies leveraged
- AWS Lambda for containerized, stateless compute 
- s3 for event coordination 
- Conda for up-to-date python packages
- cloudpickle for shipping functions back and forth

## Design 
We're trying to mimic the python 3.x futures interface as much as makes sense

http://pythonhosted.org/futures/

NOTE: The interfaces are close but not identical, because **DISTRIBUTED
COMPUTATION IS HARD**. The cloud is stormy! 


### Limitations [known ahead of time]:

- low limit of simultaneous workers (maybe 2k if you reserve ahead)
- finite amount of time per worker (300 seconds)
- possibly slow deploy process
- high latency if cold process
- challenges in supporting entire python / anaconda stack


## To Do:
- [ ] Get errors out of cloudwatch
- [ ] package everything into an executor that will take config info
- [x] Time latency and throughput of 1000 invocations
- [ ] Add more packages to conda runtime
- [ ] thread logging through 
- [x] Capture exceptions inside of the jobrunner script and thread them back out
- [ ] Benchmark dgemm
- [ ] Get a modern openblas running on the runtime 
- [ ] How to handle retries
- [x] Route job invocation ID through as well
- [ ] Should we distinguish between exeptions involved in the remote code invocaton
      and exceptions triggered by the call code
- [ ] Can we serialize futures? 
- [ ] Permissions for a new user -- IAM policies? How to constrain? 
- [ ] Is there a tasklet / greenlet version of urllib that will work better /
      be a faster backend to botocore? Right now it seems we're limited by
      our ability to dispatch / launch those jobs. Or is this a 
      aws rate-limiting issue? Performance seems to have really slowed
      down when we switched to the s3-backed job synchronization

## future 
- [ ] Investigate using EMR as a better backend for execution for long-running jobs
- [ ] how to handle upgrades


## security concerns
- At the moment, you're using my gzipped environment from my bucket. That could
be compromised, or I could be mailicious, and then I'd have access to parts of your
aws account
- Even if your account was locked down, you are then unpickling code returned
by this remote process
