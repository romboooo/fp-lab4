module ORM.QueryBuilder
open System
open Npgsql
open ORM.Schema

/// <summary>
/// Направление сортировки
/// </summary>
type SortDirection = 
    /// <summary>Сортировка по возрастанию</summary>
    | Ascending
    /// <summary>Сортировка по убыванию</summary>
    | Descending
/// <summary>
/// Операторы сравнения для условий
/// </summary>
[<RequireQualifiedAccess>]
type ComparisonOperator = 
    | Equals
    | NotEquals
    | GreaterThan
    | LessThan
    | GreaterThanOrEqual
    | LessThanOrEqual
    | Like
    | ILike

/// <summary>
/// Условие для фильтрации запросов
/// </summary>
type Condition = 
    | Binary of
        column: string *
        operator: ComparisonOperator *
        value: obj option 
    | And of Condition * Condition
    | Or of Condition * Condition
    | IsNull of column: string
    | IsNotNull of column: string
/// <summary>
/// Типы SQL-запросов
/// </summary>
type QueryType = 
    | Select of SelectQuery
    | Insert of InsertQuery
    | Update of UpdateQuery
    | Delete of DeleteQuery

/// <summary>
/// Структура SELECT запроса
/// </summary>
and SelectQuery = {
    Table: string
    Columns: string list  // пустой список будет означать *
    Where: Condition option
    OrderBy: (string * SortDirection) list
    Limit: int option
    Offset: int option
}
/// <summary>
/// Структура INSERT запроса
/// </summary>
and InsertQuery = {
    Table: string
    Columns: (string * obj) list  // имя колонки + значение
    Returning: string list option  
}
/// <summary>
/// Структура UPDATE запроса
/// </summary>
and UpdateQuery = {
    Table: string
    Set: (string * obj) list 
    Where: Condition option
}
/// <summary>
/// Структура DELETE запроса
/// </summary>
and DeleteQuery = {
    Table: string
    Where: Condition option
}
/// <summary>
/// Состояние построителя запросов
/// </summary>
type QueryBuilderState = {
    QueryType: QueryType
    Parameters: (string * obj) list
    ParameterCounter: int
}
/// <summary>
/// Модуль для построения условий запросов
/// </summary>
[<RequireQualifiedAccess>]
module Condition =
    /// <summary>
    /// Создает бинарное условие
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="operator">Оператор сравнения</param>
    /// <param name="value">Значение для сравнения</param>
    /// <returns>Бинарное условие</returns>
    let binary column operator value = 
        Binary(column, operator, Some value)
    
    /// <summary>
    /// Создает условие равенства
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="value">Значение для сравнения</param>
    /// <returns>Условие равенства</returns>
    let equals column value = 
        binary column ComparisonOperator.Equals value
    
    /// <summary>
    /// Создает условие неравенства
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="value">Значение для сравнения</param>
    /// <returns>Условие неравенства</returns>
    let notEquals column value = 
        binary column ComparisonOperator.NotEquals value
    
    /// <summary>
    /// Создает условие "больше"
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="value">Значение для сравнения</param>
    /// <returns>Условие "больше"</returns>
    let greaterThan column value = 
        binary column ComparisonOperator.GreaterThan value
    
    /// <summary>
    /// Создает условие "меньше"
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="value">Значение для сравнения</param>
    /// <returns>Условие "меньше"</returns>
    let lessThan column value = 
        binary column ComparisonOperator.LessThan value
    
    /// <summary>
    /// Создает условие поиска по шаблону
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="pattern">Шаблон поиска</param>
    /// <returns>Условие LIKE</returns>
    let like column pattern = 
        binary column ComparisonOperator.Like pattern
    
    /// <summary>
    /// Создает регистронезависимое условие поиска по шаблону
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <param name="pattern">Шаблон поиска</param>
    /// <returns>Условие ILIKE</returns>
    let iLike column pattern = 
        binary column ComparisonOperator.ILike pattern
    
    /// <summary>
    /// Создает условие проверки на NULL
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <returns>Условие IS NULL</returns>
    let isNull column = 
        IsNull column
    
    /// <summary>
    /// Создает условие проверки на NOT NULL
    /// </summary>
    /// <param name="column">Имя столбца</param>
    /// <returns>Условие IS NOT NULL</returns>
    let isNotNull column = 
        IsNotNull column
    
    /// <summary>
    /// Создает логическое И из двух условий
    /// </summary>
    /// <param name="cond1">Первое условие</param>
    /// <param name="cond2">Второе условие</param>
    /// <returns>Условие И</returns>
    let and' cond1 cond2 = 
        And(cond1, cond2)
    
    /// <summary>
    /// Создает логическое ИЛИ из двух условий
    /// </summary>
    /// <param name="cond1">Первое условие</param>
    /// <param name="cond2">Второе условие</param>
    /// <returns>Условие ИЛИ</returns>
    let or' cond1 cond2 = 
        Or(cond1, cond2)
    
    /// <summary>
    /// Создает логическое И из списка условий
    /// </summary>
    /// <param name="conditions">Список условий</param>
    /// <returns>Условие И всех условий</returns>
    /// <exception cref="System.Exception">Выбрасывается если список пуст</exception>
    let all conditions =
        match conditions with
        | [] -> failwith "At least one condition required"
        | [cond] -> cond
        | head::tail ->
            tail |> List.fold (fun acc cond -> And(acc, cond)) head
    
    /// <summary>
    /// Создает логическое ИЛИ из списка условий
    /// </summary>
    /// <param name="conditions">Список условий</param>
    /// <returns>Условие ИЛИ всех условий</returns>
    /// <exception cref="System.Exception">Выбрасывается если список пуст</exception>
    let any conditions = 
        match conditions with
        | [] -> failwith "At least one condition required"
        | [cond] -> cond
        | head::tail ->
            tail |> List.fold (fun acc cond -> Or(acc, cond)) head

/// <summary>
/// Модуль для построения SQL-запросов
/// </summary>
[<RequireQualifiedAccess>]
module Query = 
    /// <summary>
    /// Генерирует новое имя параметра
    /// </summary>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Имя параметра и обновленное состояние</returns>
    let newParamName (state: QueryBuilderState) =
        let name = sprintf "p%d" state.ParameterCounter
        name, { state with ParameterCounter = state.ParameterCounter + 1 }
    
    /// <summary>
    /// Добавляет параметр в состояние построителя
    /// </summary>
    /// <param name="name">Имя параметра</param>
    /// <param name="value">Значение параметра</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    let addParam name value (state: QueryBuilderState) =
        { state with Parameters = (name, value)::state.Parameters }
    
    /// <summary>
    /// Начинает построение SELECT запроса
    /// </summary>
    /// <param name="table">Имя таблицы</param>
    /// <returns>Начальное состояние построителя для SELECT</returns>
    let select table =
        { QueryType = Select {
            Table = table
            Columns = []
            Where = None
            OrderBy = []
            Limit = None
            Offset = None
          }
          Parameters = []
          ParameterCounter = 0 }
    
    /// <summary>
    /// Устанавливает список столбцов для SELECT запроса
    /// </summary>
    /// <param name="columnNames">Список имен столбцов</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не SELECT</exception>
    let columns columnNames state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Columns = columnNames } }
        | _ -> failwith "columns can only be used with SELECT queries"
    
    /// <summary>
    /// Добавляет условие WHERE к запросу
    /// </summary>
    /// <param name="condition">Условие фильтрации</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос INSERT</exception>
    let where condition state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Where = Some condition } }
        | Update query ->
            { state with QueryType = Update { query with Where = Some condition } }
        | Delete query ->
            { state with QueryType = Delete { query with Where = Some condition } }
        | Insert _ -> failwith "WHERE clause not supported for INSERT"
    
    /// <summary>
    /// Добавляет сортировку к SELECT запросу
    /// </summary>
    /// <param name="column">Имя столбца для сортировки</param>
    /// <param name="direction">Направление сортировки</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не SELECT</exception>
    let orderBy column direction state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with OrderBy = (column, direction)::query.OrderBy } }
        | _ -> failwith "ORDER BY can only be used with SELECT queries"
    
    /// <summary>
    /// Добавляет ограничение количества строк к SELECT запросу
    /// </summary>
    /// <param name="n">Максимальное количество строк</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не SELECT</exception>
    let limit n state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Limit = Some n } }
        | _ -> failwith "LIMIT can only be used with SELECT queries"
    
    /// <summary>
    /// Добавляет смещение к SELECT запросу
    /// </summary>
    /// <param name="n">Смещение выборки</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не SELECT</exception>
    let offset n state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Offset = Some n } }
        | _ -> failwith "OFFSET can only be used with SELECT queries"
    
    /// <summary>
    /// Начинает построение INSERT запроса
    /// </summary>
    /// <param name="table">Имя таблицы</param>
    /// <returns>Начальное состояние построителя для INSERT</returns>
    let insert table =
        { QueryType = Insert {
            Table = table
            Columns = []
            Returning = None
          }
          Parameters = []
          ParameterCounter = 0 }
    
    /// <summary>
    /// Устанавливает значения для вставки в INSERT запрос
    /// </summary>
    /// <param name="columnValues">Список пар (столбец, значение)</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не INSERT</exception>
    let values (columnValues: (string * obj) list) state =
        match state.QueryType with
        | Insert query ->
            { state with QueryType = Insert { query with Columns = columnValues } }
        | _ -> failwith "values can only be used with INSERT queries"
    
    /// <summary>
    /// Добавляет RETURNING clause к INSERT запросу
    /// </summary>
    /// <param name="columns">Список столбцов для возврата</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не INSERT</exception>
    let returning columns state =
        match state.QueryType with
        | Insert query ->
            { state with QueryType = Insert { query with Returning = Some columns } }
        | _ -> failwith "RETURNING can only be used with INSERT queries"
    
    /// <summary>
    /// Начинает построение UPDATE запроса
    /// </summary>
    /// <param name="table">Имя таблицы</param>
    /// <returns>Начальное состояние построителя для UPDATE</returns>
    let update table =
        { QueryType = Update {
            Table = table
            Set = []
            Where = None
          }
          Parameters = []
          ParameterCounter = 0 }
    
    /// <summary>
    /// Устанавливает значения для обновления в UPDATE запрос
    /// </summary>
    /// <param name="columnValues">Список пар (столбец, новое значение)</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>Обновленное состояние построителя</returns>
    /// <exception cref="System.Exception">Выбрасывается если запрос не UPDATE</exception>
    let set (columnValues: (string * obj) list) state =
        match state.QueryType with
        | Update query ->
            { state with QueryType = Update { query with Set = columnValues } }
        | _ -> failwith "set can only be used with UPDATE queries"
    
    /// <summary>
    /// Начинает построение DELETE запроса
    /// </summary>
    /// <param name="table">Имя таблицы</param>
    /// <returns>Начальное состояние построителя для DELETE</returns>
    let delete table =
        { QueryType = Delete {
            Table = table
            Where = None
          }
          Parameters = []
          ParameterCounter = 0 }

/// <summary>
/// Модуль для генерации SQL из состояний запросов
/// </summary>
[<RequireQualifiedAccess>]
module SqlGenerator =
    /// <summary>
    /// Преобразует оператор сравнения в строку SQL
    /// </summary>
    /// <param name="op">Оператор сравнения</param>
    /// <returns>Строковое представление оператора</returns>
    let private operatorToString = function
        | ComparisonOperator.Equals -> "="
        | ComparisonOperator.NotEquals -> "<>"
        | ComparisonOperator.GreaterThan -> ">"
        | ComparisonOperator.LessThan -> "<"
        | ComparisonOperator.GreaterThanOrEqual -> ">="
        | ComparisonOperator.LessThanOrEqual -> "<="
        | ComparisonOperator.Like -> "LIKE"
        | ComparisonOperator.ILike -> "ILIKE"
    
    /// <summary>
    /// Преобразует условие в SQL и обновляет состояние параметров
    /// </summary>
    /// <param name="condition">Условие для преобразования</param>
    /// <param name="state">Текущее состояние параметров</param>
    /// <returns>SQL строка условия и обновленное состояние</returns>
    let rec private conditionToSql condition (state: QueryBuilderState) =
        match condition with
        | Binary(column, op, Some value) ->
            let paramName, newState = Query.newParamName state
            let sql = sprintf "%s %s @%s" column (operatorToString op) paramName
            sql, newState |> Query.addParam paramName value
        | Binary(column, op, None) ->
            sprintf "%s %s NULL" column (operatorToString op), state
        | And(cond1, cond2) ->
            let sql1, state1 = conditionToSql cond1 state
            let sql2, state2 = conditionToSql cond2 state1
            sprintf "(%s) AND (%s)" sql1 sql2, state2
        | Or(cond1, cond2) ->
            let sql1, state1 = conditionToSql cond1 state
            let sql2, state2 = conditionToSql cond2 state1
            sprintf "(%s) OR (%s)" sql1 sql2, state2
        | IsNull column -> sprintf "%s IS NULL" column, state
        | IsNotNull column -> sprintf "%s IS NOT NULL" column, state
    
    /// <summary>
    /// Преобразует список столбцов в SQL строку
    /// </summary>
    /// <param name="columns">Список имен столбцов</param>
    /// <returns>SQL строка для SELECT части</returns>
    let private columnsToSql = function
        | [] -> "*"
        | cols -> String.Join(", ", cols)
    
    /// <summary>
    /// Преобразует список сортировок в SQL строку
    /// </summary>
    /// <param name="orderByList">Список пар (столбец, направление)</param>
    /// <returns>SQL строка ORDER BY</returns>
    let private orderByToSql orderByList =
        orderByList
        |> List.map (fun (col, dir) -> 
            let direction = 
                match dir with 
                | Ascending -> "ASC" 
                | Descending -> "DESC"
            sprintf "%s %s" col direction)
        |> function
            | [] -> ""
            | items -> sprintf " ORDER BY %s" (String.Join(", ", items))
    
    /// <summary>
    /// Строит SQL строку LIMIT и OFFSET
    /// </summary>
    /// <param name="limit">Ограничение количества строк</param>
    /// <param name="offset">Смещение выборки</param>
    /// <returns>SQL строка LIMIT/OFFSET</returns>
    let private buildLimitOffset limit offset =
        let limitPart = 
            match limit with
            | Some n -> sprintf " LIMIT %d" n
            | None -> ""
        let offsetPart =
            match offset with
            | Some n -> sprintf " OFFSET %d" n
            | None -> ""
        limitPart + offsetPart
    
    /// <summary>
    /// Генерирует SQL для SELECT запроса
    /// </summary>
    /// <param name="query">Структура SELECT запроса</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>SQL строка и список параметров</returns>
    let private generateSelect (query: SelectQuery) (state: QueryBuilderState) =
        let columnsSql = columnsToSql query.Columns
        let baseSql = sprintf "SELECT %s FROM %s" columnsSql query.Table
        let (whereSql, stateWithWhere) = 
            match query.Where with
            | Some cond -> conditionToSql cond state
            | None -> "", state
        let sqlWithWhere = 
            if String.IsNullOrEmpty whereSql then baseSql
            else baseSql + " WHERE " + whereSql
        let orderBySql = orderByToSql query.OrderBy
        let limitOffsetSql = buildLimitOffset query.Limit query.Offset
        let finalSql = sqlWithWhere + orderBySql + limitOffsetSql
        let parameters =
            stateWithWhere.Parameters
            |> List.rev
            |> List.mapi (fun i (name, value) ->
                NpgsqlParameter(name, value))
        finalSql, parameters
    
    /// <summary>
    /// Генерирует SQL для INSERT запроса
    /// </summary>
    /// <param name="query">Структура INSERT запроса</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>SQL строка и список параметров</returns>
    /// <exception cref="System.Exception">Выбрасывается если нет данных для вставки</exception>
    let private generateInsert (query: InsertQuery) (state: QueryBuilderState) =
        if List.isEmpty query.Columns then
            failwith "INSERT query must have at least one column-value pair"
        let columns = query.Columns |> List.map fst |> String.concat ", "
        let paramNames = 
            query.Columns 
            |> List.mapi (fun i _ -> sprintf "@p%d" i)
            |> String.concat ", "
        let baseSql = sprintf "INSERT INTO %s (%s) VALUES (%s)" query.Table columns paramNames
        let returningSql =
            match query.Returning with
            | Some cols -> sprintf " RETURNING %s" (String.Join(", ", cols))
            | None -> ""
        let finalSql = baseSql + returningSql
        let parameters =
            query.Columns
            |> List.mapi (fun i (_, value) ->
                NpgsqlParameter(sprintf "p%d" i, value))
        finalSql, parameters
    
    /// <summary>
    /// Генерирует SQL для UPDATE запроса
    /// </summary>
    /// <param name="query">Структура UPDATE запроса</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>SQL строка и список параметров</returns>
    /// <exception cref="System.Exception">Выбрасывается если нет данных для обновления</exception>
    let private generateUpdate (query: UpdateQuery) (state: QueryBuilderState) =
        if List.isEmpty query.Set then
            failwith "UPDATE query must have at least one SET clause"
        let (setClause, stateWithSet) =
            query.Set
            |> List.fold (fun (clauses, accState) (col, value) ->
                let paramName, newState = Query.newParamName accState
                let clause = sprintf "%s = @%s" col paramName
                let updatedState = newState |> Query.addParam paramName value
                clause::clauses, updatedState
            ) ([], state)
        let setClauseSql = setClause |> List.rev |> String.concat ", "
        let baseSql = sprintf "UPDATE %s SET %s" query.Table setClauseSql
        let (whereSql, stateWithWhere) = 
            match query.Where with
            | Some cond -> conditionToSql cond stateWithSet
            | None -> "", stateWithSet
        let finalSql = 
            if String.IsNullOrEmpty whereSql then baseSql
            else baseSql + " WHERE " + whereSql
        let parameters =
            stateWithWhere.Parameters
            |> List.rev
            |> List.mapi (fun i (name, value) ->
                NpgsqlParameter(name, value))
        finalSql, parameters
    
    /// <summary>
    /// Генерирует SQL для DELETE запроса
    /// </summary>
    /// <param name="query">Структура DELETE запроса</param>
    /// <param name="state">Текущее состояние построителя</param>
    /// <returns>SQL строка и список параметров</returns>
    let private generateDelete (query: DeleteQuery) (state: QueryBuilderState) =
        let baseSql = sprintf "DELETE FROM %s" query.Table
        let (whereSql, stateWithWhere) = 
            match query.Where with
            | Some cond -> conditionToSql cond state
            | None -> "", state
        let finalSql = 
            if String.IsNullOrEmpty whereSql then baseSql
            else baseSql + " WHERE " + whereSql
        let parameters =
            stateWithWhere.Parameters
            |> List.rev
            |> List.map (fun (name, value) ->
                NpgsqlParameter(name, value))
        finalSql, parameters
    
    /// <summary>
    /// Генерирует SQL из состояния построителя запросов
    /// </summary>
    /// <param name="state">Состояние построителя запросов</param>
    /// <returns>SQL строка и список параметров Npgsql</returns>
    let generate (state: QueryBuilderState) : string * NpgsqlParameter list =
        match state.QueryType with
        | Select query -> generateSelect query state
        | Insert query -> generateInsert query state
        | Update query -> generateUpdate query state
        | Delete query -> generateDelete query state

/// <summary>
/// Модуль вспомогательных функций для типобезопасной работы с записями
/// </summary>
[<AutoOpen>]
module TypeSafeHelpers =
    open Microsoft.FSharp.Reflection
    open ORM
    
    /// <summary>
    /// Проверяет является ли тип опциональным
    /// </summary>
    /// <param name="typ">Тип для проверки</param>
    /// <returns>true если тип Option<_>, иначе false</returns>
    let private isOptionType (typ: System.Type) =
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<option<_>>
    
    /// <summary>
    /// Извлекает значение из опционального типа
    /// </summary>
    /// <param name="value">Значение опционального типа</param>
    /// <returns>Внутреннее значение или null</returns>
    let private getValueFromOption (value: obj) : obj =
        if value = null then null
        else
            let typ = value.GetType()
            if isOptionType typ then
                let cases = FSharpType.GetUnionCases(typ)
                let case, fields = FSharpValue.GetUnionFields(value, typ)
                if case.Name = "Some" then
                    fields.[0] 
                else
                    null
            else
                value
    
    /// <summary>
    /// Преобразует запись в список пар (поле, значение) для INSERT
    /// </summary>
    /// <param name="record">Запись для преобразования</param>
    /// <returns>Список пар (имя поля в нижнем регистре, значение)</returns>
    let toInsertValues (record: 'T) : (string * obj) list =
        let recordType = typeof<'T>
        if not (FSharpType.IsRecord(recordType)) then
            failwithf "Type %s is not an F# record" recordType.Name
        
        FSharpType.GetRecordFields(recordType)
        |> Array.map (fun prop ->
            let value = FSharpValue.GetRecordField(record, prop)
            let propName = prop.Name
            
            let dbValue = 
                if isOptionType prop.PropertyType then
                    match getValueFromOption value with
                    | null -> box DBNull.Value
                    | v -> v
                elif value = null then
                    box DBNull.Value
                else
                    value
            
            (propName.ToLower(), dbValue))
        |> Array.toList
    
    /// <summary>
    /// Преобразует запись в список пар (поле, значение) для UPDATE
    /// </summary>
    /// <param name="record">Запись для преобразования</param>
    /// <param name="updateFields">Список полей для обновления</param>
    /// <returns>Список пар (имя поля в нижнем регистре, значение)</returns>
    let toUpdateValues (record: 'T) (updateFields: string list) : (string * obj) list =
        let recordType = typeof<'T>
        if not (FSharpType.IsRecord(recordType)) then
            failwithf "Type %s is not an F# record" recordType.Name
        
        let fieldsToUpdate = Set.ofList (updateFields |> List.map (fun s -> s.ToLower()))
        
        FSharpType.GetRecordFields(recordType)
        |> Array.filter (fun prop -> fieldsToUpdate.Contains(prop.Name.ToLower()))
        |> Array.map (fun prop ->
            let value = FSharpValue.GetRecordField(record, prop)
            let propName = prop.Name
            
            let dbValue = 
                if isOptionType prop.PropertyType then
                    match getValueFromOption value with
                    | null -> box DBNull.Value
                    | v -> v
                elif value = null then
                    box DBNull.Value
                else
                    value
            
            (propName.ToLower(), dbValue))
        |> Array.toList
    
    /// <summary>
    /// Валидирует запись перед выполнением операций с базой данных
    /// </summary>
    /// <param name="record">Запись для валидации</param>
    /// <param name="tableName">Имя таблицы (не используется, для совместимости)</param>
    /// <returns>Результат валидации: Ok с записью или Error с сообщением</returns>
    let validateRecord (record: 'T) (tableName: string) =
        try
            let recordType = typeof<'T>
            
            if not (FSharpType.IsRecord(recordType)) then
                Error (sprintf "Type %s is not an F# record" recordType.Name)
            else
                let properties = recordType.GetProperties()
                
                let errors = ResizeArray<string>()
                
                let primaryKeyProps = 
                    properties
                    |> Array.filter (fun prop -> 
                        prop.GetCustomAttributes(typeof<PrimaryKeyAttribute>, false).Length > 0)
                
                for pkProp in primaryKeyProps do
                    let value = pkProp.GetValue(record)
                    if value = null then
                        errors.Add(sprintf "Primary key field '%s' cannot be null" pkProp.Name)
                
                for prop in properties do
                    let isOption = isOptionType prop.PropertyType
                    let isPrimaryKey = prop.GetCustomAttributes(typeof<PrimaryKeyAttribute>, false).Length > 0
                    
                    if not isOption && not isPrimaryKey then
                        let value = prop.GetValue(record)
                        if value = null then
                            errors.Add(sprintf "Required field '%s' cannot be null" prop.Name)
                
                if errors.Count > 0 then
                    Error (String.concat "; " errors)
                else
                    Ok record
        with ex ->
            Error (sprintf "Validation error: %s" ex.Message)