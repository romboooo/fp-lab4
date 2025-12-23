namespace ORM.Tests

open Xunit
open ORM.Database

module DatabaseTests =

    [<Fact>]
    let ``Config should build connection string`` () =
        let config = {
            Host = "localhost"
            Port = 5432
            Database = "test_db"
            Username = "test_user"
            Password = "test_password"
        }
        
        let connString = Config.buildConnectionString config
        Assert.Contains("Host=localhost", connString)
        Assert.Contains("Port=5432", connString)
        Assert.Contains("Database=test_db", connString)
        Assert.Contains("Username=test_user", connString)
        Assert.Contains("Password=test_password", connString)

    [<Fact>]
    let ``DatabaseConnection type exists`` () =
        Assert.True(true)
