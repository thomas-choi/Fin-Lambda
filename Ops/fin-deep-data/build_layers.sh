#!/usr/bin/env bash
# build_layers.sh -- build the three fin-deep-data Lambda layers (python3.13).
#
#   ./build_layers.sh            # all three
#   ./build_layers.sh core yf    # only these
#
# Called by the repo-root Makefile targets finDeepCore.zip / finDeepYf.zip /
# finDeepWeb.zip. It never touches the finCron or finPort313 layers.
#
# Why a script and not three plain pip lines in the Makefile:
#
# 1. Cross-build. The local interpreter is 3.10, so every install needs
#    --platform manylinux2014_x86_64 --python-version 3.13 --only-binary=:all:,
#    which also turns a missing cp313 wheel into a build failure instead of a
#    source build against the wrong Python.
# 2. De-duplication. yfinance depends on pandas/numpy/requests, so a plain
#    install of requirements_deep_yf.txt drags in a second 100 MB copy of what
#    finDeepCore already carries. Resolution still runs with the full
#    dependency graph; afterwards every top-level entry finDeepCore provides is
#    deleted from the yf and web trees. finDeepCore is therefore mandatory on
#    any function that mounts finDeepYf or finDeepWeb.
# 3. The size ceiling. Layer zips must stay under LAYER_ZIP_MAX_MB (80 MB) --
#    AWS itself refuses a direct upload over 50 MB and caps a function's
#    layers at 250 MB unzipped combined. Pruning tests/ and __pycache__ takes
#    finDeepCore from 124 MB to about 100 MB unzipped. Stripping .so files would
#    save 7 MB more and must not be done -- see prune_layer().
set -euo pipefail

cd "$(dirname "$0")"
HERE="$PWD"
REPO_ROOT="$(cd ../.. && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build/fin-deep-data}"
PY_VERSION=3.13
LAYER_ZIP_MAX_MB="${LAYER_ZIP_MAX_MB:-80}"
PIP="${PIP:-pip3}"

declare -A ZIP_NAME=([core]=finDeepCore [yf]=finDeepYf [web]=finDeepWeb)

site_packages() { echo "$BUILD_DIR/$1/python/lib/python$PY_VERSION/site-packages"; }

install_layer() {
    local layer="$1" dest constraint=()
    dest="$(site_packages "$layer")"
    # The yf and web trees resolve against finDeepCore's pins, so a shared
    # dependency (pandas, numpy, requests, pytz) resolves to the version core
    # actually ships. Without this, pip picks the newest release and the
    # de-duplication below leaves its stale dist-info behind, which would make
    # the layer advertise a pandas version that is not on the path.
    [ "$layer" = core ] || constraint=(-c "$HERE/requirements_deep_core.txt")
    rm -rf "${BUILD_DIR:?}/${layer:?}"
    mkdir -p "$dest"
    $PIP install -r "$HERE/requirements_deep_$layer.txt" "${constraint[@]}" \
        --platform manylinux2014_x86_64 \
        --implementation cp --python-version "$PY_VERSION" \
        --only-binary=:all: \
        --no-compile \
        -t "$dest"
}

prune_layer() {
    local dest
    dest="$(site_packages "$1")"
    # Test suites and bytecode are dead weight in a layer.
    find "$dest" -type d \( -name tests -o -name test -o -name __pycache__ \) -prune -exec rm -rf {} +
    find "$dest" -name '*.pyc' -delete
    # DO NOT strip the .so files here. It saved ~7 MB and broke numpy outright:
    # `strip --strip-unneeded` rewrote numpy.libs/libscipy_openblas64_*.so into
    # "ELF load command address/offset not page-aligned", because auditwheel
    # patches those bundled manylinux libraries with a non-standard page
    # alignment that strip does not preserve. numpy then reports it as
    # "you should not try to import numpy from its source directory", which names
    # neither strip nor OpenBLAS -- the real cause is only in the chained
    # "Original error was:" line. Cost us the first invoke of the service
    # (doc/OPERATIONS.md 8.5). 7 MB is not worth it; the zips fit without it.
}

dedupe_against_core() {
    local dest core_sp entry name
    dest="$(site_packages "$1")"
    core_sp="$(site_packages core)"
    if [ ! -d "$core_sp" ]; then
        echo "ERROR: build core first -- $1 is de-duplicated against it" >&2
        exit 1
    fi
    for entry in "$dest"/*; do
        name="$(basename "$entry")"
        if [ -e "$core_sp/$name" ]; then
            rm -rf "$entry"
        fi
    done
}

zip_layer() {
    local layer="$1" zip_path unzipped zipped
    zip_path="$REPO_ROOT/${ZIP_NAME[$layer]}.zip"
    rm -f "$zip_path"
    (cd "$BUILD_DIR/$layer" && zip -qr9 "$zip_path" python)
    unzipped=$(du -sm "$(site_packages "$layer")" | cut -f1)
    zipped=$(du -m "$zip_path" | cut -f1)
    printf '%-14s %3s MB unzipped  %3s MB zipped  -> %s\n' \
        "${ZIP_NAME[$layer]}" "$unzipped" "$zipped" "$zip_path"
    if [ "$zipped" -gt "$LAYER_ZIP_MAX_MB" ]; then
        echo "ERROR: ${ZIP_NAME[$layer]}.zip is ${zipped} MB, over the ${LAYER_ZIP_MAX_MB} MB ceiling" >&2
        exit 1
    fi
}

layers=("$@")
[ ${#layers[@]} -eq 0 ] && layers=(core yf web)

for layer in "${layers[@]}"; do
    [ -n "${ZIP_NAME[$layer]:-}" ] || { echo "unknown layer '$layer' (core|yf|web)" >&2; exit 2; }
    echo "==> $layer"
    install_layer "$layer"
    prune_layer "$layer"
    [ "$layer" = core ] || dedupe_against_core "$layer"
    zip_layer "$layer"
done
