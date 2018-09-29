DEBUGGER ?= 
BRZ ?= $(shell which brz)
PYTHON ?= $(shell which python)
SETUP ?= ./setup.py
PYDOCTOR ?= pydoctor
CTAGS ?= ctags
PYLINT ?= pylint
RST2HTML ?= rst2html
TESTS ?= Hg -s bp.hg

all:: build 

build::
	$(SETUP) build

build-inplace::

install::
	$(SETUP) install

clean::
	$(SETUP) clean
	rm -f *.so

check:: build-inplace 
	BRZ_PLUGINS_AT=hg@$(shell pwd) $(DEBUGGER) $(PYTHON) $(PYTHON_OPTIONS) $(BRZ) $(BRZ_OPTIONS) selftest $(TEST_OPTIONS) $(TESTS)

check-all::
	$(MAKE) check TESTS="^bzrlib.plugins.hg. Hg"

check-verbose::
	$(MAKE) check TEST_OPTIONS=-v

check-one::
	$(MAKE) check TEST_OPTIONS=--one

check-random::
	$(MAKE) check TEST_OPTIONS="--random=now --verbose --one"

show-plugins::
	BRZ_PLUGINS_AT=hg@$(shell pwd) $(BRZ) plugins -v

lint::
	$(PYLINT) -f parseable *.py */*.py

tags::
	$(CTAGS) -R .

ctags:: tags

coverage::
	$(MAKE) check BRZ_OPTIONS="--coverage ,coverage"

.PHONY: update-pot po/bzr-hg.pot
update-pot: po/bzr-hg.pot

TRANSLATABLE_PYFILES:=$(shell find . -name '*.py' \
		| grep -v 'tests/' \
		)

po/bzr-hg.pot: $(PYFILES) $(DOCFILES)
	BRZ_PLUGINS_AT=hg@$(shell pwd) bzr export-pot \
          --plugin=hg > po/bzr-hg.pot
	echo $(TRANSLATABLE_PYFILES) | xargs \
	  xgettext --package-name "bzr-hg" \
	  --msgid-bugs-address "<bazaar@lists.canonical.com>" \
	  --copyright-holder "Canonical Ltd <canonical-bazaar@lists.canonical.com>" \
	  --from-code ISO-8859-1 --sort-by-file --join --add-comments=i18n: \
	  -d bzr-hg -p po -o bzr-hg.pot
