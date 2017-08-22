# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.

# Dest. to copy generated web files.
PYWREN_DOCS_DIR   ?=${PYWREN_DOCS} 

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
ifdef PYWREN_DOCS
	$(MAKE) html
	${MKDIR_P} ${PYWREN_DOCS}
	cp -r ./_build/html/* ${PYWREN_DOCS}
else
	@echo 1>&2 "PYWREN_DOCS must be set to the root of the pywren documentation website repo."
	false
endif
