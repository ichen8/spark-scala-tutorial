package util

/**
 * We use a case class to define the schema, as required by Spark SQL.
 */
case class Abbrev(abbrev: String, name: String)
