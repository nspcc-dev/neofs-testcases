## Запуск тесткейсов

### Первичная подготовка

1. Установить neofs-cli 
    - `git clone git@github.com:nspcc-dev/neofs-node.git`
    - `cd neofs-node` 
    - `make`
    - `sudo cp bin/neofs-cli /usr/local/bin/neofs-cli` or add path to bin/neofs-cli

2. Установить cdn-authmate
    - `git clone git@github.com:nspcc-dev/cdn-authmate.git`
    - `cd cdn-authmate`
    - `make build`
    - `sudo cp bin/cdn-authmate /usr/local/bin/cdn-authmate` or add path to bin/cdn-authmate

3. Устаносить зависимости для Testcases
    - `pip3 install robotframework`
    - `pip3 install pexpect`
    - `pip3 install requests`
    - `pip3 install boto3`

(pip3 заменить на соответсвующий менеджер пакетов python в системе).

При этом должен быть запущен dev-env с тестируемым окружением.

### Запуск тесткейсов

1. Выпольнить `make run`

2. Логи будут доступны в папке artifacts/ после завершения тестов с любым из статусов.


### Запуск произвольного тесткейса

Для запуска произвольного тесткейса нужно выполнить команду:
`robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/<testsuite name>.robot `

Для запуска доступны следущие сценарии:

 * acl_basic.robot - базовый ACL
 * acl_extended.robot - extended ACL
 * acl_baearer.robot - Bearer Token ACL
 * object_complex.robot - операции над простым объектом
 * object_simple.robot - операции над большим объектом
 * withdraw.robot - оперция Deposit и Withdraw с счета NeoFS
 * netmap_simple.robot - проверка Placement policy
 * replication.robot - базовый тесткейс проверки репликации объектов


### Запуск smoke-тестов

Есть сьют со smoke-тестами для CDN-гейтов `robot/testsuites/smoke/selectelcdn_smoke.robot`.
Ему требуются отдельные переменные, в отличие от сьютов NeoFS, которые запускаются на
девэнве. Чтобы библиотеки кейвордов их использовали, нужно установить переменную
окружения
```
export ROBOT_PROFILE=selectel_smoke
```
По умолчанию кейворды используют переменные из файла `robot/resources/lib/neofs_int_vars.py`.
```
robot --outputdir artifacts/ robot/testsuites/smoke/selectelcdn_smoke.robot
```


### Генерация документации

Для генерации документации по шагам:
```
python3 -m robot.libdoc robot/resources/lib/neofs.py docs/NeoFS_Library.html
python3 -m robot.libdoc robot/resources/lib/payment_neogo.py docs/Payment_Library.html
```

Для генерации документации по тесткейсам:
```
python3 -m robot.testdoc robot/testsuites/integration/ docs/testcases.html
```

## Создание тесткейсов

### Source code overview

`robot/` - Files related/depended on Robot Framework.

`robot/resources/` - All resources (Robot Framework Keywords, Python Libraries, etc) which could be used for creating test suites.

`robot/resources/lib/` - Common Python Libraries depended on Robot Framework (with Keywords). For example neofs.py, payment.py.

`robot/variables/` - All variables for tests. It is possible to add the auto-loading logic of parameters from the smart-contract in the future. Contain python files.

`robot/testsuites/` - Robot Test Suites and Test Cases.

`robot/testsuites/integration/` - Integration test suites and test cases

`robot/testsuites/fi/` - Fault Injection test suites and test cases

### Code style

Robot Framework keyword should use space as a separator between particular words

The name of the library function in Robot Framework keyword usage and the name of the same function in the Python library must be identical.

The name of GLOBAL VARIABLE must be in UPPER CASE, the underscore ('_')' symbol must be used as a separator between words.

The name of local variable must be in lower case, the underscore symbol must be used as a separator between words.

The names of Python variables, functions and classes must comply with accepted rules, in particular:
Name of variable/function must be in lower case with underscore symbol between words
Name of class must start with a capital letter. It is not allowed to use underscore symbol in name, use capital for each particular word.
For example: NeoFSConf

Name of other variables should not be ended with underscore symbol

On keywords definition, one should specify variable type, e.g. path: str

### Robot style

Следует всегда заполнять секции [Tags] и [Documentation] для Testcase'ов и Documentation для Test Suite'ов.

### Robot-framework User Guide

http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html
