# type-safe ORM на языке f#

## Структура
1. src/Database.fs - подключение к БД и выполнение запросов
2. src/SqlParser.fs - парсинг схемы БД 
3. src/TypeGenerator.fs - генерация типов F# из схемы
4. src/QueryBuilder.fs - построение SQL запросов
5. src/core/ORMCore.fs - высокоуровневый API для CRUD операций
6. src/DataMapper.fs - маппинг f# типов -> sql типы и наоборот

## Пример использования

Билдим проект
```sh
dotnet clean; dotnet build;
```
Создаем типы из схемы (сохраняются в generated/GeneratedTypes.fs)
```sh
dotnet run -- --generate-types
```
Запуск

```sh
dotnet run //WIP
```

## конфигурация .env файла (необходимо поместить в корень проекта)

```
POSTGRES_USER=username
POSTGRES_PASSWORD=password
POSTGRES_DB=dbname
POSTGRES_HOST=localhost
POSTGRES_PORT=6767
```

## Как это работает?

1. Подключаемся к базе данных
2. Получаем схемы таблиц из вашей бд (они должны быть доступны из information_schema и table_schema должен быть 'public')
3. Генерируем f# типы в generated/GeneratedTypes.fs (на этапе build) (+ маппинг)
4. Компилируется ORMCore - публичная API (библиотека) с доступными CRUD (cudi) методами (create, delete, insert, update)
5. Вызываем метод, используя поля сгенерированного типа
6. (под капотом) Вызывается QueryBuilder, формируя type-safe запрос (runtime проверка)
7. Запрос отправляется в DataBase.fs, откуда идет непосредствеено в базу данных
8. Мапинг результата (если select)
9. Логирование ошибок (по наличию)

## Доступные sql типы
src/Schema.fs
```f#
type ColumnType =
    | Int
    | BigInt
    | Varchar of maxLength: int option  
    | Text                           
    | Boolean
    | Json                            
    | Date
```

## Доступные sql запросы

```f#
type QueryType = 
    | Select of SelectQuery
    | Insert of InsertQuery
    | Update of UpdateQuery
    | Delete of DeleteQuery
```

## Roadmap

Этап 1: Безопасная конфигурация ✅
  * Подключение к PostgreSQL через Npgsql ✅
  * Чтение данных из .env ✅
  * Базовая проверка соединения ✅

Этап 2: Парсинг схемы базы данных (SqlParser)✅
  Цель: Получить структуру таблиц из БД 
  1. Получить список всех таблиц в схеме public ✅
  2. Для каждой таблицы получить: ✅
      * Название таблицы 
      * Список колонок с: 
          * Именем колонки
          * Типом данных PostgreSQL
          * Признаком NULL/NOT NULL
          * Признаком первичного ключа
          * Ограничениями (varchar(100), decimal(10,2), etc.)
  3. Сохранить эту структуру в F# типах ✅
     
Этап 3: генератор типов на этапе build ✅
  Цель: представить таблицы f# как типы 
  1. Генерация типов на основе схемы из Этапа 2: ✅
      * type User = { id: int; username: string; email: string }
      * type Product = { id: int; name: string; price: decimal }
  2. Маппинг типов PostgreSQL → F#: ✅
      * INTEGER → int
      * VARCHAR → string
      * DECIMAL → decimal
      * BOOLEAN → bool
      * TIMESTAMP → DateTime
      * и т.д.
  3. Генерация типов во build ✅
  4. Валидация типов после билда ✅
     
Этап 4: query builder
  1. Базовый DSL для построения запросов: ✅
      * SELECT * FROM table WHERE condition
      * INSERT INTO table (columns) VALUES (values)
      * UPDATE table SET column = value WHERE condition
      * DELETE FROM table WHERE condition
  2. Параметризованные запросы (защита от SQL-инъекций) ✅
  3. Поддержка простых условий (WHERE, ORDER BY, LIMIT) ✅
     
Этап 5: ORM Core - CRUD операции
  Цель: Реализовать базовые методы ORM
  1. Create/Insert: db.Users.Insert({ ... }) ⚠️TODO
  2. Read/Select: ✅
      * db.Users.FindAll()
      * db.Users.FindBy(fun u -> u.Id = 1)
      * db.Users.Where(fun u -> u.Age > 18).ToList()
  3. Update: db.Users.Update(1, {| Name = "New Name" |}) ⚠️TODO
  4. Delete: db.Users.Delete(1) ⚠️ TODO



## Проблемы
  1. Конфликт зависимостей uGet с FSharp.TypeProviders.SDK (deprecated: при неудачной попытке реализовать type Provider)
  2. Не ясно как гарантировать подключение к бд на этапе компиляции (deprecated: при неудачной попытке реализовать type Provider)
