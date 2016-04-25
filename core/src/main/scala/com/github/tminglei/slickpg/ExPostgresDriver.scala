package com.github.tminglei.slickpg

import scala.slick.ast.{TableNode, TableExpansion, Select, ColumnOption}
import scala.slick.compiler.{InsertCompiler, Phase, QueryCompiler}
import scala.slick.lifted.PrimaryKey
import slick.driver.{InsertBuilderResult, JdbcDriver, JdbcProfile, PostgresDriver}
import slick.ast._


trait ExPostgresDriver extends JdbcDriver with PostgresDriver { driver =>

  override val simple = new SimpleQL {}
  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)
  override def createUpsertBuilder(node: Insert): InsertBuilder =
    if (useNativeUpsert) new NativeUpsertBuilder(node) else new super.UpsertBuilder(node)

  protected lazy val useNativeUpsert = capabilities contains JdbcProfile.capabilities.insertOrUpdate
  override protected lazy val useTransactionForUpsert = !useNativeUpsert
  override protected lazy val useServerSideUpsertReturning = useNativeUpsert

  override lazy val upsertCompiler = QueryCompiler(Phase.assignUniqueSymbols, new InsertCompiler(UpsertCompiler.NoDates), new JdbcInsertCodeGen(createUpsertBuilder))

  object UpsertCompiler extends InsertCompiler{
    /** Determines which columns to include in the `Insert` and mapping nodes
      * created by `InsertCompiler`. */
    trait Mode extends (FieldSymbol => Boolean)

    /** Include all columns. For use in forced inserts and merges. */
    case object AllColumns extends Mode {
      def apply(fs: FieldSymbol) = true
    }
    /** Include only non-AutoInc columns. For use in standard (soft) inserts. */
    case object NoDates extends Mode {
      def apply(fs: FieldSymbol) = !List("created_at", "updated_at").contains(fs.name)
    }
    /** Include only primary keys. For use in the insertOrUpdate emulation. */
    case object PrimaryKeys extends Mode {
      def apply(fs: FieldSymbol) = fs.options.contains(ColumnOption.PrimaryKey)
    }
  }

  trait SimpleQL extends super.SimpleQL {
    type InheritingTable = driver.InheritingTable
  }

  trait InheritingTable { sub: Table[_] =>
    val inherited: Table[_]
  }
  /***********************************************************************
    *                          for upsert support
    ***********************************************************************/

  class NativeUpsertBuilder(ins: Insert) extends super.InsertBuilder(ins) {
    private val (nonPkAutoIncSyms, insertingSyms) = syms.toIndexedSeq.partition { s =>
      s.options.contains(ColumnOption.AutoInc) && !(s.options contains ColumnOption.PrimaryKey) }
    private lazy val (pkSyms, softSyms) = insertingSyms.partition(_.options.contains(ColumnOption.PrimaryKey))
    private lazy val pkNames = pkSyms.map { fs => quoteIdentifier(fs.name) }
    private lazy val softNames = softSyms.map { fs => quoteIdentifier(fs.name) }

    override def buildInsert: InsertBuilderResult = {
      val insert = s"insert into $tableName (${insertingSyms.mkString(",")}) values (${insertingSyms.map(_ => "?").mkString(",")})"
      val onConflict = "on conflict (" + pkNames.mkString(", ") + ")"
      val doSomething = if (softNames.isEmpty) "do nothing" else "do update set " + softNames.map(n => s"$n=EXCLUDED.$n").mkString(",")
      val padding = if (nonPkAutoIncSyms.isEmpty) "" else "where ? is null or ?=?"
      new InsertBuilderResult(table, s"$insert $onConflict $doSomething $padding", syms)
    }

    override def transformMapping(n: Node) = reorderColumns(n, insertingSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms)
  }

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override protected val columns: Iterable[ColumnDDLBuilder] = {
      (if(table.isInstanceOf[InheritingTable]) {
        val hColumns = table.asInstanceOf[InheritingTable].inherited.create_*.toSeq.map(_.name.toLowerCase)
        table.create_*.filterNot(s => hColumns.contains(s.name.toLowerCase))
      } else table.create_*)
        .map(fs => createColumnDDLBuilder(fs, table))
    }
    override protected val primaryKeys: Iterable[PrimaryKey] = {
      if(table.isInstanceOf[InheritingTable]) {
        val hTable = table.asInstanceOf[InheritingTable].inherited
        val hPrimaryKeys = hTable.primaryKeys.map(pk => PrimaryKey(table.tableName + "_" + pk.name, pk.columns))
        hTable.create_*.find(_.options.contains(ColumnOption.PrimaryKey))
          .map(s => PrimaryKey(table.tableName + "_PK", IndexedSeq(Select(tableNode, s))))
          .map(Iterable(_) ++ hPrimaryKeys ++ table.primaryKeys)
          .getOrElse(hPrimaryKeys ++ table.primaryKeys)
      } else table.primaryKeys
    }

    override protected def createTable: String = {
      if(table.isInstanceOf[InheritingTable]) {
        val hTable = table.asInstanceOf[InheritingTable].inherited
        val hTableNode = hTable.toNode.asInstanceOf[TableExpansion].table.asInstanceOf[TableNode]
        s"${super.createTable} inherits (${quoteTableName(hTableNode)})"
      } else super.createTable
    }
  }
}

object ExPostgresDriver extends ExPostgresDriver