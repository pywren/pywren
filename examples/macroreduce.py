import pywren

if __name__ == "__main__":

    lambda_exec = pywren.lambda_executor()
    remote_exec = pywren.remote_executor()


    def increment(x):
        return x+1

    x = [1, 2, 3, 4]
    futures = lambda_exec.map(increment, x)

    def reduce_func(x):
        return sum(x)

    reduce_future = remote_exec.reduce(reduce_func, futures)
    print reduce_future.result()
