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


## security concerns
- At the moment, you're using my gzipped environment from my bucket. That could
be compromised, or I could be mailicious, and then I'd have access to parts of your
aws account
- Even if your account was locked down, you are then unpickling code returned
by this remote process
