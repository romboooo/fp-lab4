namespace ORM.Tests

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
        Assert.Single(parameters)
        Assert.Equal("p0", parameters.[0].ParameterName)
        Assert.Equal(25, parameters.[0].Value :?> int)

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
        Assert.Single(parameters)
        Assert.Equal(100, parameters.[0].Value :?> int)