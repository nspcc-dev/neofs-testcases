# DevEnv variables
export NEOFS_MORPH_DISABLE_CACHE=true
pushd ../neofs-dev-env
export `make env`
popd

export PYTHONPATH=${PYTHONPATH}:${VIRTUAL_ENV}/neofs-keywords/lib:${VIRTUAL_ENV}/neofs-keywords/robot:~/neofs-testcases/robot/resources/lib
