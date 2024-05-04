package hungrytech.kotlinquerydslbatch.reader

import com.querydsl.jpa.JPQLQuery
import com.querydsl.jpa.impl.JPAQuery
import com.querydsl.jpa.impl.JPAQueryFactory
import jakarta.persistence.EntityManager
import jakarta.persistence.EntityManagerFactory
import jakarta.persistence.EntityTransaction
import java.util.concurrent.CopyOnWriteArrayList
import org.springframework.batch.item.database.AbstractPagingItemReader
import org.springframework.util.ClassUtils

sealed class QuerydslPagingItemReader<T>(
    val entityManagerFactory: EntityManagerFactory,
    pageSize: Int,
    val transacted: Boolean = true,
    val query: (JPAQueryFactory) -> JPAQuery<T>
) : AbstractPagingItemReader<T>() {

    init {
        this.name = ClassUtils.getShortName(QuerydslPagingItemReader::class.java)
        this.pageSize = pageSize
    }

    private val jobPropertyMap: Map<String, Any> = mutableMapOf()

    protected lateinit var entityManager: EntityManager

    override fun doOpen() {
        super.doOpen()

        val entityManager = entityManagerFactory.createEntityManager(jobPropertyMap)
            ?: throw IllegalStateException("EntityManagerFactory must set up EntityManager")

        this.entityManager = entityManager
    }

    override fun doReadPage() {
        val transaction = getTx()

        val query = createQuery().apply {
            offset(page * pageSize.toLong())
                .limit(pageSize.toLong())
        }

        initResults()

        fetchQuery(query, transaction)
    }

    override fun doClose() {
        entityManager.close()
        super.doClose()
    }

    fun getTx(): EntityTransaction? {
        if (transacted) {
            val transaction = entityManager.transaction

            transaction.begin()

            entityManager.flush()
            entityManager.clear()
            return transaction
        }

        return null
    }

    open fun createQuery(): JPAQuery<T> {
        val queryFactory = JPAQueryFactory(entityManager)

        return query(queryFactory)
    }

    fun initResults() {
        if (results == null || results.isEmpty()) {
            results = CopyOnWriteArrayList()
        } else {
            results.clear()
        }
    }

    /**
     * where 의 조건은 id max/min 을 이용한 제한된 범위를 가지게 한다
     * @param query
     * @param tx
     */
    fun fetchQuery(query: JPQLQuery<T>, tx: EntityTransaction?) {
        if (transacted) {
            results.addAll(query.fetch())
            tx?.commit()
            return
        }

        query.fetch()
            .forEach {
                entityManager.detach(it)
                results.add(it)
            }
    }
}
