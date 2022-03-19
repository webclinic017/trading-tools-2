#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

rm -rf $DIR/venv
mkdir -p $DIR/venv
python3 -m venv venv

. ./venv/bin/activate

pip install wheel
pip install -r dependencies.txt
pip freeze > requirements.txt