namespace ORM.Tests

open System
open Xunit
open ORM.Schema
open ORM.TypeGenerator

module TypeGeneratorTests =
    
    [<Fact>]
    let ``CodeGenerator should convert names correctly`` () =
        let pascal = CodeGenerator.toPascalCase "user_orders"
        Assert.Equal("UserOrders", pascal)
        
        let camel = CodeGenerator.toCamelCase "user_orders"
        Assert.Equal("userOrders", camel)
        
        let camel2 = CodeGenerator.toCamelCase "id"
        Assert.Equal("id", camel2)
    
    [<Fact>]
    let ``CodeGenerator should map PostgreSQL types to F# types`` () =
        let intCol = { 
            Name = "id"
            DataType = Int
            IsNullable = false
            IsPrimaryKey = true
            MaxLength = None
        }
        
        let nullableStringCol = {
            Name = "description"
            DataType = Text
            IsNullable = true
            IsPrimaryKey = false
            MaxLength = None
        }
        
        let floatCol = {
            Name = "score"
            DataType = Float
            IsNullable = false
            IsPrimaryKey = false
            MaxLength = None
        }
        
        let intType = CodeGenerator.pgTypeToFSharpType intCol
        Assert.Equal("int", intType)
        
        let stringType = CodeGenerator.pgTypeToFSharpType nullableStringCol
        Assert.Equal("string option", stringType)
        
        let floatType = CodeGenerator.pgTypeToFSharpType floatCol
        Assert.Equal("float", floatType)
