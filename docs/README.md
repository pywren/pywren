### How to edit the docs

Make sure you have sphinx installed
```
pip install sphinx
```

The docs are generated from the `*.rst` in `./source/`.

To auto-generate documentation, execute
```
cd docs/
sphinx-apidoc -o source/ ../pywren
```


To build the html files, execute `make html`.


##Misc.
Most of the configuration is done in `conf.py`
