
### Локальный запуск тесткейсов
1. Устаносить зависимости (только для первого запуска):
    - pip3 install robotframework
    - pip3 install neocore
    - pip3 install requests

(pip3 заменить на соответсвующий менеджер пакетов python в системе).

При этом должен быть запущен dev-env с тестируемым окружением.

Из корня dev-env выполнить команду:
```
docker cp wallets/wallet.json main_chain:/wallets/
```

2. Выпольнить `make run`

3. Логи будут доступны в папке artifacts/ после завершения тестов с любым из статусов.

### Запуск произвольного тесткейса
Для запуска произвольного тесткейса нужно выполнить команду:
`robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/<testsuite name>.robot `

Для запуска доступны следущие сценарии:
 * acl_basic.robot - базовый ACL
 * acl_extended.robot - extended ACL
 * object_complex.robot - операции над простым объектом
 * object_simple.robot - операции над большим объектом


### Запуск тесткейсов в докере
1. Задать переменные окружения для работы с dev-env:
```
    export REG_USR=<registry_user>
    export REG_PWD=<registry_pass>
    export JF_TOKEN=<JF_token>
```

2. Выполнить `make build`

3. Выполнить `make run_docker`

4. Логи будут доступны в папке artifacts/ после завершения тестов с любым из статусов.

### Запуск тесткейсов в докере с произвольными коммитами

На данный момент доступны произовльные коммиты для NeoFS Node и NeoFS CLI.
Для этого достаточно задать переменные окружения перед запуском `make build`.
```
export BUILD_NEOFS_NODE=<commit or branch>
export BUILD_CLI=<commit or branch>
```
