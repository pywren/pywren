### How to edit the docs

Make sure you have sphinx installed
```
pip install sphinx
```

The docs are generated from the `*.rst` in `./source/`.

To auto-generate documentation, make sure you are in this directory (`/docs/`)` and execute
```
sphinx-apidoc -o source/ ../pywren
```

To build the html files, make sure you have environment variable `$PYWREN_DOCS` set to whatever the pywren docs repo is.

Execute `make update`.

Or, to build the html files into only the `/_build` directory of this repo execute `make html`.


### Misc.
Most of the configuration is done in `conf.py`
