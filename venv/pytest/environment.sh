# DevEnv variables
export NEOFS_MORPH_DISABLE_CACHE=true
export WALLET_PASS=password
popd > /dev/null

export PYTHONPATH=${PYTHONPATH}:${VIRTUAL_ENV}/../pytest_tests/lib:${VIRTUAL_ENV}/../neofs-testlib/
