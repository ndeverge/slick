package org.scalaquery.ql.extended

import java.util.UUID
import org.scalaquery.ql._
import org.scalaquery.ql.basic._
import org.scalaquery.ast._
import org.scalaquery.util._
import org.scalaquery.session.{PositionedResult, PositionedParameters}

class PostgresDriver extends ExtendedProfile { self =>

  type ImplicitT = ExtendedImplicitConversions[PostgresDriver]
  type TypeMapperDelegatesT = PostgresTypeMapperDelegates

  val Implicit = new ExtendedImplicitConversions[PostgresDriver] {
    implicit val scalaQueryDriver = self
  }

  val typeMapperDelegates = new PostgresTypeMapperDelegates

  override def createQueryBuilder(query: Query[_, _]) = new PostgresQueryBuilder(query, this)
  override def buildTableDDL(table: AbstractBasicTable[_]): DDL = new PostgresDDLBuilder(table, this).buildDDL
}

object PostgresDriver extends PostgresDriver

class PostgresTypeMapperDelegates extends BasicTypeMapperDelegates {
  override val byteArrayTypeMapperDelegate = new BasicTypeMapperDelegates.ByteArrayTypeMapperDelegate {
    override val sqlTypeName = "BYTEA"
  }
  override val uuidTypeMapperDelegate = new BasicTypeMapperDelegates.UUIDTypeMapperDelegate {
    override def setValue(v: UUID, p: PositionedParameters) = p.setObject(v, sqlType)
    override def setOption(v: Option[UUID], p: PositionedParameters) = p.setObjectOption(v, sqlType)
    override def nextValue(r: PositionedResult) = r.nextObject().asInstanceOf[UUID]
    override def updateValue(v: UUID, r: PositionedResult) = r.updateObject(v)
    override def valueToSQLLiteral(value: UUID) = "'" + value + "'"
  }

  override val byteTypeMapperDelegate = new ByteTypeMapperDelegate

  /* PostgreSQL does not have a TINYINT type, so we use SMALLINT instead. */
  class ByteTypeMapperDelegate extends BasicTypeMapperDelegates.ByteTypeMapperDelegate {
    override def sqlTypeName = "SMALLINT"
  }
}

class PostgresQueryBuilder(_query: Query[_, _], profile: PostgresDriver) extends BasicQueryBuilder(_query, profile) {

  override protected val concatOperator = Some("||")

  override protected def appendTakeDropClause(take: Option[Int], drop: Option[Int], b: SQLBuilder) = (take, drop) match {
    case (Some(t), Some(d)) => b += " LIMIT " += t += " OFFSET " += d
    case (Some(t), None) => b += " LIMIT " += t
    case (None, Some(d)) => b += " OFFSET " += d
    case _ =>
  }
}

class PostgresDDLBuilder(table: AbstractBasicTable[_], profile: PostgresDriver) extends BasicDDLBuilder(table, profile) {
  import profile.sqlUtils._

  protected class PostgresColumnDDLBuilder(column: RawNamedColumn) extends BasicColumnDDLBuilder(column) {
    override def appendColumn(sb: StringBuilder) {
      sb append quoteIdentifier(column.name) append ' '
      if(autoIncrement) {
        sb append "SERIAL"
        autoIncrement = false
      }
      else sb append sqlType
      appendOptions(sb)
    }
  }

  override protected def createColumnDDLBuilder(c: RawNamedColumn) = new PostgresColumnDDLBuilder(c)
}
