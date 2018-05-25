## How to edit the docs

### Prerequisites
Make sure you have sphinx installed
```
pip install sphinx
```

### Editing .rst files.
The docs are generated from the `*.rst` in `./source/`.

A good source for .rst syntax is (here)[http://www.sphinx-doc.org/en/stable/rest.html]

Each item in the doc page is formatted based on the docstring. To add the doc for a function, use
```
 .. autofunction:: pywren.MODULE.FUNCTION_NAME
```
This will generate a doc section based on the docstring for the given function. Spinx is sensitive to spacing, and you need an empty line before autofunction.

You could also use 
```
..automethod:: pywren.MODULE.CLASS_NAME.FUNCTION_NAME
```
but the only difference I've noticed is that this doesn't display `pywren.MODULE`.
### Formatting a docstring
```
def example(foo, bar):
    """
    This will be displayed under the function signature and can describe what the func does.
    :param foo: These will be formatted in the parameter list
    :param bar: These will be formatted in the parameter list
    :return: What this returns.
    :rtype: The type of value this function returns
    Usage::
      #sample usage of this function. This wil lbe syntax highlighted.
      example(1, 2)
    """
    return foo + bar
```
### Build
To build the html files, execute `make update`.

### Misc.
Most of the configuration is done in `conf.py`
