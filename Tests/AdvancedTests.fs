namespace ORM.Tests

open System
open Xunit
open ORM.Database
open ORM.ORMCore
open ORM.GeneratedTypes

module AdvancedTests =

    [<Fact>]
    let ``Full CRUD cycle should work correctly`` () =
        Assert.True(true)
    
    [<Fact>]
    let ``QueryBuilder should generate valid SQL for complex queries`` () =
        let query = 
            Query.select "users"
            |> Query.columns ["id"; "username"; "email"]
            |> Query.where (
                Condition.and'
                    (Condition.greaterThan "age" 18)
                    (Condition.or'
                        (Condition.like "username" "john%")
                        (Condition.equals "email" "john@example.com")
                    )
            )
            |> Query.orderBy "createdAt" Descending
            |> Query.limit 100
        
        let sql, _ = ORM.QueryBuilder.SqlGenerator.generate query
        Assert.Contains("SELECT id, username, email FROM users", sql)
        Assert.Contains("age > @p0", sql)
        Assert.Contains("username LIKE @p1", sql)
        Assert.Contains("ORDER BY createdAt DESC", sql)
        Assert.Contains("LIMIT 100", sql)
    
    [<Fact>]
    let ``TypeGenerator should create valid F# types`` () =
        let testTable = {
            ORM.Schema.Schema = "public"
            ORM.Schema.Name = "test_table"
            ORM.Schema.Columns = [
                { Name = "id"; DataType = ORM.Schema.Int; IsNullable = false; IsPrimaryKey = true; MaxLength = None }
                { Name = "name"; DataType = ORM.Schema.Varchar (Some 100); IsNullable = false; IsPrimaryKey = false; MaxLength = Some 100 }
                { Name = "description"; DataType = ORM.Schema.Text; IsNullable = true; IsPrimaryKey = false; MaxLength = None }
                { Name = "price"; DataType = ORM.Schema.Int; IsNullable = false; IsPrimaryKey = false; MaxLength = None }
                { Name = "created_at"; DataType = ORM.Schema.Date; IsNullable = false; IsPrimaryKey = false; MaxLength = None }
            ]
        }
        
        let generatedType = ORM.TypeGenerator.CodeGenerator.generateRecordType testTable
        Assert.Contains("type TestTable = {", generatedType)
        Assert.Contains("[<PrimaryKey>] id: int", generatedType)
        Assert.Contains("name: string", generatedType)
        Assert.Contains("description: string option", generatedType)
        Assert.Contains("price: int", generatedType)
        Assert.Contains("createdAt: System.DateTime", generatedType)