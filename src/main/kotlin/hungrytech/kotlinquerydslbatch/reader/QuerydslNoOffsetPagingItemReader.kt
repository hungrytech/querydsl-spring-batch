package hungrytech.kotlinquerydslbatch.reader

import com.querydsl.jpa.JPQLQuery
import com.querydsl.jpa.impl.JPAQuery
import com.querydsl.jpa.impl.JPAQueryFactory
import hungrytech.kotlinquerydslbatch.reader.options.QuerydslNoOffsetNumberOptions
import hungrytech.kotlinquerydslbatch.reader.options.QuerydslNoOffsetOptions
import jakarta.persistence.EntityManagerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.util.CollectionUtils

class QuerydslNoOffsetPagingItemReader<T>(
    entityManagerFactory: EntityManagerFactory,
    pageSize: Int,
    transacted: Boolean = true,
    private val options: QuerydslNoOffsetOptions<T>,
    query: (JPAQueryFactory) -> JPAQuery<T>
) : QuerydslPagingItemReader<T>(
    entityManagerFactory = entityManagerFactory,
    pageSize = pageSize,
    transacted = transacted,
    query = query
) {

    override fun doReadPage() {
        val transaction = getTx()

        val query: JPQLQuery<T> = createQuery().limit(pageSize.toLong())

        initResults()

        fetchQuery(query, transaction)

        resetCurrentIdIfNotLastPage()
    }

    override fun createQuery(): JPAQuery<T> {
        val queryFactory = JPAQueryFactory(entityManager)
        val query: JPAQuery<T> = query(queryFactory)
        options.initKeys(query, page) // 제일 첫번째 페이징시 시작해야할 ID 찾기

        return options.createQuery(query, page)
    }

    private fun resetCurrentIdIfNotLastPage() {
        if (isNotEmptyResults()) {
            options.resetCurrentId(results.last())
        }
    }

    // 조회결과가 Empty이면 results에 null이 담긴다
    private fun isNotEmptyResults(): Boolean {
        return results.isNotEmpty() && results[0] != null
    }
}

/**
 * NoOffset 방식으로 페이징 처리하는 Reader
 * T DB 로부터 Read 하는 Type
 * N No Offset으로 조회하는 필드의 Type
 */
class QuerydslNoOffsetIdPagingItemReader<T, N>(
    entityManagerFactory: EntityManagerFactory,
    pageSize: Int,
    transacted: Boolean = false,
    private val options: QuerydslNoOffsetNumberOptions<T, N>,
    query: (JPAQueryFactory) -> JPAQuery<T>
) : QuerydslPagingItemReader<T>(
    entityManagerFactory = entityManagerFactory,
    pageSize = pageSize,
    transacted = transacted,
    query = query
) where N : Number, N : Comparable<*> {

    override fun doReadPage() {
        val transaction = getTx()

        val query: JPQLQuery<T> = createQuery().limit(pageSize.toLong())

        initResults()

        fetchQuery(query, transaction)

        resetCurrentIdIfNotLastPage()
    }

    override fun createQuery(): JPAQuery<T> {
        val queryFactory = JPAQueryFactory(entityManager)
        val query: JPAQuery<T> = query(queryFactory)
        options.initKeys(query, page) // 제일 첫번째 페이징시 시작해야할 ID 찾기

        return options.createQuery(query, page)
    }

    private fun resetCurrentIdIfNotLastPage() {
        if (isNotEmptyResults()) {
            options.resetCurrentId(results.last())
        }
    }

    // 조회결과가 Empty이면 results에 null이 담긴다
    private fun isNotEmptyResults(): Boolean {
        return results.isNotEmpty() && results[0] != null
    }
}
