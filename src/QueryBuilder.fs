module ORM.QueryBuilder

open System
open Npgsql
open ORM.Schema

type SortDirection = 
    | Ascending
    | Descending

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

type Condition = 
    | Binary of
        column: string *
        operator: ComparisonOperator *
        value: obj option 
    | And of Condition * Condition
    | Or of Condition * Condition
    | IsNull of column: string
    | IsNotNull of column: string

type QueryType = 
    | Select of SelectQuery
    | Insert of InsertQuery
    | Update of UpdateQuery
    | Delete of DeleteQuery

and SelectQuery = {
    Table: string
    Columns: string list  // пустой список будет означать *
    Where: Condition option
    OrderBy: (string * SortDirection) list
    Limit: int option
    Offset: int option
}

and InsertQuery = {
    Table: string
    Columns: (string * obj) list  // имя колонки + значение
    Returning: string list option  
}

and UpdateQuery = {
    Table: string
    Set: (string * obj) list 
    Where: Condition option
}

and DeleteQuery = {
    Table: string
    Where: Condition option
}

type QueryBuilderState = {
    QueryType: QueryType
    Parameters: (string * obj) list
    ParameterCounter: int
}

[<RequireQualifiedAccess>]
module Condition =
    let binary column operator value = 
        Binary(column, operator, Some value)
    
    let equals column value = 
        binary column ComparisonOperator.Equals value
    
    let notEquals column value = 
        binary column ComparisonOperator.NotEquals value
    
    let greaterThan column value = 
        binary column ComparisonOperator.GreaterThan value
    
    let lessThan column value = 
        binary column ComparisonOperator.LessThan value
    
    let like column pattern = 
        binary column ComparisonOperator.Like pattern
    
    let iLike column pattern = 
        binary column ComparisonOperator.ILike pattern
    
    let isNull column = 
        IsNull column
    
    let isNotNull column = 
        IsNotNull column
    
    let and' cond1 cond2 = 
        And(cond1, cond2)
    
    let or' cond1 cond2 = 
        Or(cond1, cond2)

    let all conditions =
        match conditions with
        | [] -> failwith "At least one condition required"
        | [cond] -> cond
        | head::tail ->
            tail |> List.fold (fun acc cond -> And(acc, cond)) head
    
    let any conditions = 
        match conditions with
        | [] -> failwith "At least one condition required"
        | [cond] -> cond
        | head::tail ->
            tail |> List.fold (fun acc cond -> Or(acc, cond)) head

[<RequireQualifiedAccess>]
module Query = 
    let newParamName (state: QueryBuilderState) =
        let name = sprintf "p%d" state.ParameterCounter
        name, { state with ParameterCounter = state.ParameterCounter + 1 }
    
    let addParam name value (state: QueryBuilderState) =
        { state with Parameters = (name, value)::state.Parameters }

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
    
    let columns columnNames state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Columns = columnNames } }
        | _ -> failwith "columns can only be used with SELECT queries"
    
    let where condition state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Where = Some condition } }
        | Update query ->
            { state with QueryType = Update { query with Where = Some condition } }
        | Delete query ->
            { state with QueryType = Delete { query with Where = Some condition } }
        | Insert _ -> failwith "WHERE clause not supported for INSERT"
    
    let orderBy column direction state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with OrderBy = (column, direction)::query.OrderBy } }
        | _ -> failwith "ORDER BY can only be used with SELECT queries"
    
    let limit n state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Limit = Some n } }
        | _ -> failwith "LIMIT can only be used with SELECT queries"
    
    let offset n state =
        match state.QueryType with
        | Select query ->
            { state with QueryType = Select { query with Offset = Some n } }
        | _ -> failwith "OFFSET can only be used with SELECT queries"
    
    let insert table =
        { QueryType = Insert {
            Table = table
            Columns = []
            Returning = None
          }
          Parameters = []
          ParameterCounter = 0 }
    
    let values (columnValues: (string * obj) list) state =
        match state.QueryType with
        | Insert query ->
            { state with QueryType = Insert { query with Columns = columnValues } }
        | _ -> failwith "values can only be used with INSERT queries"
    
    let returning columns state =
        match state.QueryType with
        | Insert query ->
            { state with QueryType = Insert { query with Returning = Some columns } }
        | _ -> failwith "RETURNING can only be used with INSERT queries"
    
    let update table =
        { QueryType = Update {
            Table = table
            Set = []
            Where = None
          }
          Parameters = []
          ParameterCounter = 0 }
    
    let set (columnValues: (string * obj) list) state =
        match state.QueryType with
        | Update query ->
            { state with QueryType = Update { query with Set = columnValues } }
        | _ -> failwith "set can only be used with UPDATE queries"
    
    let delete table =
        { QueryType = Delete {
            Table = table
            Where = None
          }
          Parameters = []
          ParameterCounter = 0 }

[<RequireQualifiedAccess>]
module SqlGenerator =
    
    let private operatorToString = function
        | ComparisonOperator.Equals -> "="
        | ComparisonOperator.NotEquals -> "<>"
        | ComparisonOperator.GreaterThan -> ">"
        | ComparisonOperator.LessThan -> "<"
        | ComparisonOperator.GreaterThanOrEqual -> ">="
        | ComparisonOperator.LessThanOrEqual -> "<="
        | ComparisonOperator.Like -> "LIKE"
        | ComparisonOperator.ILike -> "ILIKE"
    
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
    
    let private columnsToSql = function
        | [] -> "*"
        | cols -> String.Join(", ", cols)
    
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
    
    let generate (state: QueryBuilderState) : string * NpgsqlParameter list =
        match state.QueryType with
        | Select query -> generateSelect query state
        | Insert query -> generateInsert query state
        | Update query -> generateUpdate query state
        | Delete query -> generateDelete query state

[<AutoOpen>]
module TypeSafeHelpers =
    
    // Эти функции будут использоваться в ORMCore.fs
    // Они обеспечивают runtime type safety
    
    let validateRecord (record: 'T) (tableName: string) =
        // Здесь можно добавить валидацию:
        // - Проверка обязательных полей
        // - Проверка типов
        // - Проверка ограничений (varchar length и т.д.)
        // Пока просто заглушка
        Ok record
    
    let toInsertValues (record: 'T) : (string * obj) list =
        // Рефлексия для преобразования записи в список пар (колонка, значение)
        // Это runtime, но нам нужно type-safe API
        []
    
    let toUpdateValues (record: 'T) (updateFields: string list) : (string * obj) list =
        // Возвращает только указанные поля для обновления
        []