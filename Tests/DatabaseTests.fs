// tests/DatabaseTests.fs
namespace ORM.Tests

open Xunit
open ORM.Database

module DatabaseTests =

    [<Fact>]
    let ``DatabaseConnection should be created`` () =
        use connection = new DatabaseConnection()
        Assert.NotNull(connection)

    [<Fact>]
    let ``DatabaseConnection should open connection`` () =
        use connection = new DatabaseConnection()
        use conn = connection.GetOpenConnection()
        Assert.Equal(System.Data.ConnectionState.Open, conn.State)
        conn.Close()