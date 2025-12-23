namespace ORM.Tests

open Xunit
open ORM.Schema

module SqlParserTests =

    [<Fact>]
    let ``ColumnType mapping for known types`` () =
        let testCases = [
            ("integer", None, Int)
            ("bigint", None, BigInt)
            ("varchar", Some 100, Varchar (Some 100))
            ("text", None, Text)
            ("boolean", None, Boolean)
            ("json", None, Json)
            ("date", None, Date)
            ("real", None, Float)
        ]
        
        for (input, maxLen, expected) in testCases do
            let result = 
                match input.ToLower() with
                | "integer" | "int" | "int4" -> Int
                | "bigint" | "int8" -> BigInt
                | "character varying" | "varchar" -> Varchar maxLen
                | "text" -> Text
                | "boolean" | "bool" -> Boolean
                | "json" | "jsonb" -> Json
                | "date" -> Date
                | "real" | "float4" -> Float
                | _ -> Text
            
            Assert.Equal(expected, result)
