namespace ORM

open System

[<AttributeUsage(AttributeTargets.Property)>]
type PrimaryKeyAttribute() =
    inherit Attribute()