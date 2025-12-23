namespace ORM.Tests

open System
open Xunit
open ORM.QueryBuilder
open Npgsql

module QueryBuilderTests =

    [<Fact>]
    let ``Select query should generate correct SQL`` () =
        let query = Query.select "users"
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("SELECT * FROM users", sql)
        Assert.Empty(parameters)

    [<Fact>]
    let ``Select with columns should generate correct SQL`` () =
        let query = 
            Query.select "users"
            |> Query.columns ["id"; "name"]
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("SELECT id, name FROM users", sql)
        Assert.Empty(parameters)

    [<Fact>]
    let ``Select with where condition should generate correct SQL`` () =
        let query = 
            Query.select "users"
            |> Query.where (Condition.equals "age" 25)
        
        let (sql, parameters) = SqlGenerator.generate query
        
        Assert.Contains("SELECT * FROM users WHERE age = @p0", sql)
        Assert.Single(parameters) |> ignore
        Assert.Equal("p0", parameters.[0].ParameterName)
        Assert.Equal(25, parameters.[0].Value :?> int)
        ignore parameters 

    [<Fact>]
    let ``Insert query should generate correct SQL`` () =
        let query = 
            Query.insert "users"
            |> Query.values [
                "name", box "John"
                "age", box 30
            ]
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("INSERT INTO users (name, age) VALUES (@p0, @p1)", sql)
        Assert.Equal(2, parameters.Length)
        Assert.Equal("John", parameters.[0].Value :?> string)
        Assert.Equal(30, parameters.[1].Value :?> int)

    [<Fact>]
    let ``Update query should generate correct SQL`` () =
        let query = 
            Query.update "users"
            |> Query.set ["age", box 31]
            |> Query.where (Condition.equals "id" 1)
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("UPDATE users SET age = @p0 WHERE id = @p1", sql)
        Assert.Equal(2, parameters.Length)
        Assert.Equal(31, parameters.[0].Value :?> int)
        Assert.Equal(1, parameters.[1].Value :?> int)

    [<Fact>]
    let ``Delete query should generate correct SQL`` () =
        let query = 
            Query.delete "users"
            |> Query.where (Condition.greaterThan "age" 100)
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("DELETE FROM users WHERE age > @p0", sql)
        Assert.Single(parameters) |> ignore
        Assert.Equal(100, parameters.[0].Value :?> int)

    [<Fact>]
    let ``Complex nested conditions should generate correct SQL`` () =
        let query = 
            Query.select "orders"
            |> Query.where (
                Condition.and'
                    (Condition.greaterThan "total" 100)
                    (Condition.or'
                        (Condition.equals "status" "completed")
                        (Condition.and'
                            (Condition.equals "status" "pending")
                            (Condition.lessThan "created_at" (DateTime(2024, 1, 1)))
                        )
                    )
            )
        
        let (sql, parameters) = SqlGenerator.generate query
        
        Assert.Contains("SELECT * FROM orders WHERE (total > @p0) AND ((status = @p1) OR ((status = @p2) AND (created_at < @p3)))", sql)
        Assert.Equal(4, parameters.Length)

    [<Fact>]
    let ``Multiple order by clauses should generate correct SQL`` () =
        let query = 
            Query.select "users"
            |> Query.orderBy "last_name" Ascending
            |> Query.orderBy "first_name" Ascending
            |> Query.orderBy "created_at" Descending
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("ORDER BY created_at DESC, first_name ASC, last_name ASC", sql)

    [<Fact>]
    let ``Insert with null values should generate correct SQL`` () =
        let query = 
            Query.insert "users"
            |> Query.values [
                "username", box "testuser"
                "age", box null
                "email", box DBNull.Value
            ]
        
        let sql, parameters = SqlGenerator.generate query
        Assert.Contains("INSERT INTO users (username, age, email) VALUES (@p0, @p1, @p2)", sql)
        Assert.Equal(3, parameters.Length)
        
        Assert.Equal("testuser", parameters.[0].Value :?> string)
        
        let ageValue = parameters.[1].Value
        Assert.True(ageValue = null || ageValue = DBNull.Value)
        
        Assert.Equal(DBNull.Value, parameters.[2].Value :?> DBNull)
    [<Fact>]
    let ``Insert with returning all columns should generate correct SQL`` () =
        let query = 
            Query.insert "users"
            |> Query.values [
                "username", box "testuser"
                "email", box "test@example.com"
            ]
            |> Query.returning ["*"]
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("INSERT INTO users (username, email) VALUES (@p0, @p1) RETURNING *", sql)
        Assert.Equal(2, parameters.Length)

    [<Fact>]
    let ``Update with multiple set values should generate correct SQL`` () =
        let query = 
            Query.update "users"
            |> Query.set [
                "email", box "new@example.com"
                "age", box 31
                "updated_at", box DateTime.Now
            ]
            |> Query.where (Condition.equals "id" 1)
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("UPDATE users SET email = @p0, age = @p1, updated_at = @p2 WHERE id = @p3", sql)
        Assert.Equal(4, parameters.Length)

    [<Fact>]
    let ``Delete with complex condition should generate correct SQL`` () =
        let query = 
            Query.delete "users"
            |> Query.where (
                Condition.and'
                    (Condition.isNull "activated_at")
                    (Condition.lessThan "created_at" (DateTime(2023, 1, 1)))
            )
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("DELETE FROM users WHERE (activated_at IS NULL) AND (created_at < @p0)", sql)
        Assert.Single(parameters)

    [<Fact>]
    let ``Select with specific columns should generate correct SQL`` () =
        let query = 
            Query.select "users"
            |> Query.columns ["id"; "username"; "email"]
            |> Query.where (Condition.greaterThan "age" 18)
        
        let (sql, parameters) = SqlGenerator.generate query
        Assert.Contains("SELECT id, username, email FROM users WHERE age > @p0", sql)
        Assert.Single(parameters)
