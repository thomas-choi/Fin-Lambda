CC=gcc-11
CPP=g++-11
CXX=g++-11
LD=g++-11
AR=ar

cxxflags = $(CPPFLAGS) $(CXXFLAGS) -I./cplusplus \
	-g -Wall -Wextra -Werror -std=c++20 -pthread -fPIC
ldflags = $(LDFLAGS)
#  -static
arflags = rc
LDLIBS = -L./cplusplus -lgrinder -pthread -lboost_program_options

PYFLAGS = -I/usr/include/python3.8 -L/usr/lib/python3.8 -lpython3.8

# g++ -fPIC -shared -I/usr/include/python3.10 -L/usr/lib/python3.10 mathlib.cpp -lpython3.10 -o libmath.so

sources = \

objects = $(sources:.cpp=.o)

$(info V is "$(V)")

ifeq ($(V),1)
	VCXX   = $(CXX) -c
	VCXXLD = $(CXX)
	VDEPS  = $(CXX) -MM
else
	VCXX   = @echo "  COMPILE $@" && $(CXX) -c
	VCXXLD = @echo "  LINK $@" && $(CXX)
	VDEPS  = @echo "  DEPENDS $@" && $(CXX) -MM
endif

all: finWebLib.zip finSvrLib.zip finVisLib.zip finSvr2Lib.zip finDataLib.zip finCron.zip config.zip

ldaShell:
	docker build -t mylambda .

finCron.zip:
	$(RM) -rf ./python
	pip3 install -r Ops/fin-cron-data/requirements_cron.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

# Layer for portAssetsHandler, the only python3.13 function in this service.
# Note the python3.13 site-packages path -- every other target hardcodes
# python3.10, and a layer built under the wrong path is silently unimportable.
#
# The explicit --platform/--python-version flags matter: the local interpreter
# is 3.10, so a plain `pip3 install` would resolve cp310 wheels that a 3.13
# Lambda cannot load. --only-binary=:all: makes a missing cp313 wheel a build
# failure rather than a source build against the wrong Python.
finPort313.zip:
	$(RM) -rf ./python
	pip3 install -r Ops/fin-cron-data/requirements_port313.txt -t python/lib/python3.13/site-packages
# 	pip3 install -r Ops/fin-cron-data/requirements_port313.txt \
# 		--platform manylinux2014_x86_64 \
# 		--implementation cp --python-version 3.13 \
# 		--only-binary=:all: \
# 		--no-compile \
# 		-t python/lib/python3.13/site-packages
	zip -r9 $@ python/
	@echo "unzipped size: $$(du -sh python | cut -f1)  (Lambda limit: 250 MB unzipped, all layers combined)"

# ---------------------------------------------------------------------------
# fin-deep-data layers (python3.13) -- Ops/fin-deep-data.
#
# Three small layers instead of one big tree, so every zip stays well under the
# 80 MB working ceiling and each function mounts only what it imports:
#
#   finDeepCore  pandas, numpy, SQLAlchemy, PyMySQL, python-dotenv, pytz,
#                requests       101 MB unzipped / 29 MB zipped -- all 8 functions
#   finDeepYf    yfinance 0.2.58 + deps   28 MB / 10 MB -- the downloaders
#   finDeepWeb   lxml, bs4, openpyxl      14 MB /  6 MB -- the scrapers
#
# build_layers.sh does the cross-build for cp313/manylinux2014_x86_64, prunes
# tests and debug symbols, and de-duplicates the yf and web trees against core
# (so BUILD CORE FIRST). It fails the build if a zip exceeds 80 MB.
#
# These targets add nothing to and change nothing in finCron / finPort313.
finDeep: finDeepCore.zip finDeepYf.zip finDeepWeb.zip

finDeepCore.zip:
	Ops/fin-deep-data/build_layers.sh core

# No make-level dependency on finDeepCore.zip: these need core's *build tree*,
# not its zip, and rebuilding core for every one of them would waste a minute
# each time. The script stops with "build core first" if the tree is missing.
finDeepYf.zip:
	Ops/fin-deep-data/build_layers.sh yf

finDeepWeb.zip:
	Ops/fin-deep-data/build_layers.sh web

finWebLib.zip: 
	$(RM) -rf ./python
	pip3 install -r req_Web.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

finSvrLib.zip: 
	$(RM) -rf ./python
	pip3 install -r req_Server.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

finSvr2Lib.zip: 
	$(RM) -rf ./python
	pip3 install -r req_Server2.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

finVisLib.zip: 
	$(RM) -rf ./python
	pip3 install -r req_Visual.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

finDataLib.zip: 
	$(RM) -rf ./python
	pip3 install -r req_Data.txt -t python/lib/python3.10/site-packages
	zip -r9 $@ python/

config.zip:
	zip -r9 $@ configure/

clean:
	$(RM) -rf ./python config.zip myFinDataFull.zip

.PHONY: all clean finDeep finDeepCore.zip finDeepYf.zip finDeepWeb.zip
