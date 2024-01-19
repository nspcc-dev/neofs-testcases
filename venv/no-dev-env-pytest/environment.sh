# DevEnv variables
export NEOFS_MORPH_DISABLE_CACHE=true
export WALLET_PASS=password
popd > /dev/null

export PYTHONPATH=${PYTHONPATH}:${VIRTUAL_ENV}/../robot/resources/lib/:${VIRTUAL_ENV}/../robot/resources/lib/python_keywords:${VIRTUAL_ENV}/../robot/resources/lib/robot:${VIRTUAL_ENV}/../robot/variables:${VIRTUAL_ENV}/../pytest_tests/helpers:${VIRTUAL_ENV}/../pytest_tests/steps:${VIRTUAL_ENV}/../pytest_tests/resources:${VIRTUAL_ENV}/../dynamic_env_pytest_tests/lib
