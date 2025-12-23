namespace ORM

open System

/// <summary>
/// Атрибут для пометки свойства как первичного ключа
/// </summary>
[<AttributeUsage(AttributeTargets.Property)>]
type PrimaryKeyAttribute() =
    inherit Attribute()
