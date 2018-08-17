# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.

SPHINXOPTS    =
SPHINXBUILD   = python -msphinx
SPHINXPROJ    = pywren
SOURCEDIR     = ./source
BUILDDIR      = _build

MKDIR_P = mkdir -p

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile update

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

update:
	$(MAKE) html
	find ./_build/html/ -type f -name '*.html' | xargs sed -i  '' 's/<h1 class="logo"><a href="index.html">/<h1 class="logo"><a href="http:\/\/pywren.io\/">/g'
	sed -i  '' 's/<h1 class="logo"><a href="#">/<h1 class="logo"><a href="http:\/\/pywren.io\/">/g' ./_build/html/index.html
