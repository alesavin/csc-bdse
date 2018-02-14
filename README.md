## csc-bdse
Базовый проект для практики по курсу "Программная инженерия больших данных"

## Задача 1
Необходимо реализовать вариацию Persistent Storage Unit (kv-node), по сути единицы хранения данных в 
распределенной БД. Для упрощения задачи поставляются:
- модель хранения KeyValue
- интерфейс KeyValueStorageNode, предполагается что храним всегда строковые ключи и байтовый массив данных. Операция сканирования 
части/всего объема хранимого реализуется через получения хранимых ключей и далее точечной загрузки значений. Для 
kv-node определен интерфейс управления: получения статуса и прием команд (например, остановка и старт ноды)    
- HTTP API kv-node оборачивающее методы единицы хранилища
- тривиальная реализация kv-node в памяти, используется только для примеров
- клиент к HTTP API
- интеграционные тесты с использованием библиотеки testcontainers: позволяют запустить приложения в контейнерах и 
провести их тестирование. набор тестов не полон и обязателен к расширению в результате реализации задачи.   

### Требуется:  
- сделать fork проекта https://github.com/alesavin/csc-bdse и выдать доступы всем участникам команды (добавить в 
collaborators)
- сделать в fork-проекте бранч csc-bdse-task1 где будет находиться сдаваемый материал для первой задачи
- для имплементации функциональности Persistent Storage Unit предлагается использовать любую существующую СУБД, 
например, Cassandra, MySQL, Postgres, Mongo, самописную реализацию на файлах или другие, для доступа - оффициальные 
клиент или драйвер. Выбор СУБД для использования “под капотом” kv-node является частью задания, предполагается что запуск выбранной СУБД производится в контейнере 
 (разрешается использовать готовые контейнеры из Docker HUB). Необходимо учитывать требования к kv-node: 
возможность хранения существенного набора ключей (тысячи), сохранение данных после рестарта СУБД и самой ноды, 
точечное обновление по ключу, возможность получения частичного набора ключей. Варианты ограничены только 
требованиями, можно выбирать как документные, так и реляционные СУБД, возможно написание всего вручную на файлах ч
(требуется упаковка в контейнер).  
- реализовать инплементацию kv-node с использованием выбранной СУБД, в том числе инициализацию схемы (если таковая 
необходима), конфигурирования протокола общения с СУБД, тестов 
- реализовать интеграционные тесты ноды с поднятием (и выключением) в контейнерах СУБД, ноды 
- описать в INSTALL.md специфику сборки kv-node и запуска интеграционных тестов
- описать в README.md что было реализовано, описание проблем, не решенных в коде и требующих дальнейшего 
рассмотрения, неявных моментов. Обязательно добавить название и список участников команды.  
- прислать PR {your-awesome-fork-repo}/csc-bdse-task1 => {your-awesome-fork-repo}/master (добавить alesavin, 
dormidon, semkagtn, 747mmHg в ревьюеры)         

### Дополнительно:
- вторая реализация kv-node на СУБД, кардинально отличной от первой версии (MySQL 4 и MySQL 5 не подходит)
- интеграционные тесты с заливкой миллиона ключей https://www.quora.com/Data/Where-can-I-find-large-datasets-open-to-the-public






 
