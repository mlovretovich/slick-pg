package com.github.tminglei.slickpg

import slick.driver.JdbcProfile
import slick.profile.Capability
import org.junit._
import org.junit.Assert._

class PgUpsertSuite {

    object MyPostgresDriver extends ExPostgresDriver {

        // Add back `capabilities.insertOrUpdate` to enable native `upsert` support
        override protected def computeCapabilities: Set[Capability] =  (super.computeCapabilities
          + JdbcProfile.capabilities.insertOrUpdate
          )
    }

    import ExPostgresDriver.simple._

    val db = Database.forURL(url = dbUrl, driver = "org.postgresql.Driver")

    case class Bean(id: Long, col1: String, col2: Int)

    class UpsertTestTable(tag: Tag) extends Table[Bean](tag, "test_tab_upsert") {

        def id = column[Long]("id", O.AutoInc, O.PrimaryKey)
        def col1 = column[String]("col1")
        def col2 = column[Int]("col2")

        def * = (id, col1, col2) <>(Bean.tupled, Bean.unapply)
    }

    val UpsertTests = TableQuery[UpsertTestTable]

    @Test
    def testEmulateUpsertSupport: Unit = {
        val upsertSql = ExPostgresDriver.compileInsert(UpsertTests.toNode).upsert.sql
        println(s"upsert sql: $upsertSql")

        assert(upsertSql.contains("begin;"))
        db withSession { implicit session: Session =>

            UpsertTests.ddl.create
            UpsertTests.forceInsertAll(Bean(101, "aa", 3), Bean(102, "bb", 5), Bean(103, "cc", 11))
            UpsertTests.insertOrUpdate(Bean(101, "a1", 3))
            UpsertTests.insertOrUpdate(Bean(107, "dd", 7))

            assert(UpsertTests.sortBy(_.id).list sameElements Seq(Bean(1, "dd", 7),
                                                                                    Bean(101, "a1", 3),
                                                                                    Bean(102, "bb", 5),
                                                                                    Bean(103, "cc", 11))
                  )

            UpsertTests.ddl.drop
        }

    }

    @Test
    def testNativeUpsertSupport: Unit = {
        val upsertSql = MyPostgresDriver.compileInsert(UpsertTests.toNode).upsert.sql
        println(s"upsert sql: $upsertSql")
        println(MyPostgresDriver.capabilities)

        assert(upsertSql.contains("on conflict"))
        db withSession { implicit session: Session =>

            UpsertTests.ddl.create
            UpsertTests.forceInsertAll(Bean(101, "aa", 3), Bean(102, "bb", 5), Bean(103, "cc", 11))
            UpsertTests.insertOrUpdate(Bean(101, "a1", 3))
            UpsertTests.insertOrUpdate(Bean(107, "dd", 7))

            assert(UpsertTests.sortBy(_.id).list sameElements Seq(
                                                                     Bean(1, "dd", 7),
                                                                     Bean(101, "a1", 3), Bean(102, "bb", 5),
                                                                     Bean(103, "cc", 11))
                  )

            UpsertTests.ddl.drop
        }

    }

}
