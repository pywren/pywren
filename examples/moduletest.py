import pywren
import extmodule

if __name__ == "__main__":

    wrenexec = pywren.default_executor()
    def foo(x):
        return extmodule.foo_add(x)

    x = 1.0
    fut = wrenexec.call_async(foo, x)

    res = fut.result() 
    print res

