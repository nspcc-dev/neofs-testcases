# DevEnv variables
export NEOFS_MORPH_DISABLE_CACHE=true
export DEVENV_PATH="${DEVENV_PATH:-${VIRTUAL_ENV}/../../neofs-dev-env}"
pushd $DEVENV_PATH > /dev/null
export `make env`
popd > /dev/null

export PYTHONPATH=${PYTHONPATH}:${VIRTUAL_ENV}/../neofs-keywords/lib:${VIRTUAL_ENV}/../neofs-keywords/robot:${VIRTUAL_ENV}/../robot/resources/lib/python_keywords:${VIRTUAL_ENV}/../robot/resources/lib/robot:${VIRTUAL_ENV}/../robot/variables
