# PyWren

> The wrens are mostly small, brownish passerine birds in the mainly New World family Troglodytidae. ... Most wrens are small and rather inconspicuous, except for their loud and often complex songs. - Wikipedia

PyWren -- it's like a mini condor, in the cloud, for often-complex calls. 

Goal: Dispatch of small efficient tasks with as close to zero user overhead
as possible. In particular, entirely serverless -- use as many AWS services
as necessary. 

## Key technologies leveridged
- AWS Lambda for containerized, stateless compute 
- s3 for event coordination 
- Conda for up-to-date python packages
- cloudpickle for shipping functions back and forth

## Design 
We're trying to mimic the python 3.x futures interface as much as makes sense

http://pythonhosted.org/futures/

NOTE: The interfaces are close but not identical, because DISTRIBUTED
COMPUTATION IS HARD. The cloud is stormy! In particular, `submit` for python
futures takes other crap. 


res = pywren.map(func, list)

and get an object back that sorta looks like a list of futures. 

### Limitations [known ahead of time]:

- low limit of simultaneous workers (maybe 2k if you reserve ahead)
- finite amount of time per worker (300 seconds)
- possibly slow deploy process
- high latency if cold process
- challenges in supporting entire python / anaconda stack


783175685819

--role arn:aws:iam::783175685819:role/lambda_basic_execution  \


## To Do:
- [ ] Get errors out of cloudwatch
- [ ] package everything into an executor that will take config info
- [x] Time latency and throughput of 1000 invocations
- [ ] Add more packages to conda runtime
- [ ] thread logging through 
- [ ] Capture exceptions inside of the jobrunner script and thread them back out
- [ ] Benchmark dgemm
- [ ] Get a modern openblas running on the runtime 
- [ ] How to handle retries
- [ ] Route job invocation ID through as well
- [ ] Should we distinguish between exeptions involved in the remote code invocaton
      and exceptions triggered by the call code
      
## future 
- [ ] Investigate using EMR as a better backend for execution for long-running jobs
- [ ] how to handle upgrades


## security concerns
- At the moment, you're using my gzipped environment from my bucket. That could
be compromised, or I could be mailicious, and then i'd have access to parts of your
aws account
- Even if your account was locked down, you are then unpickling code returned
by this remote process
