package hungrytech.kotlinquerydslbatch.reader.options

import com.querydsl.core.types.OrderSpecifier
import com.querydsl.core.types.Path
import com.querydsl.core.types.dsl.BooleanExpression
import com.querydsl.core.types.dsl.NumberPath
import com.querydsl.core.types.dsl.StringPath
import com.querydsl.jpa.impl.JPAQuery
import io.github.oshai.kotlinlogging.KotlinLogging

abstract class QuerydslNoOffsetOptions<T>(
    protected open val field: Path<*>,
    protected val expression: Expression
) {
    protected val logger = KotlinLogging.logger { }

    private val fieldName: String by lazy {
        val fieldName = field.toString().split(findTargetFieldRegex).last()

        if (logger.isDebugEnabled()) {
            logger.debug { "fieldName= $fieldName" }
        }

        fieldName
    }

    abstract fun initKeys(query: JPAQuery<T>, page: Int)

    abstract fun createQuery(query: JPAQuery<T>, page: Int): JPAQuery<T>

    abstract fun resetCurrentId(item: T)

    protected abstract fun initFirstId(query: JPAQuery<T>)
    protected abstract fun initLastId(query: JPAQuery<T>)


    protected fun getFieldValue(item: T): Any {
        return try {
            val field = item!!.javaClass.getDeclaredField(fieldName)
            field.isAccessible = true
            field.get(item)
        } catch (e: NoSuchFieldException) {
            logger.error(e) { "Not Found or Not Access Field= $fieldName" }
            throw IllegalArgumentException("Not Found or Not Access Field")
        } catch (e: IllegalAccessException) {
            logger.error(e) { "Not Found or Not Access Field= $fieldName" }
            throw IllegalArgumentException("Not Found or Not Access Field")
        }
    }

    fun isGroupByQuery(query: JPAQuery<T>): Boolean {
        return isGroupByQuery(query.toString())
    }

    fun isGroupByQuery(sql: String): Boolean {
        return sql.contains("group by")
    }

    companion object {
        val findTargetFieldRegex = "\\.".toRegex()
    }
}


class QuerydslNoOffsetNumberOptions<T, N>(
    override val field: NumberPath<N>,
    expression: Expression
) : QuerydslNoOffsetOptions<T>(field, expression) where N : Number, N : Comparable<*> {

    private var _currentId: N? = null
    private var _lastId: N? = null

    val currentId: N get() = _currentId ?: throw IllegalStateException("CurrentId is null")
    val lastId: N get() = _lastId ?: throw IllegalStateException("LastId is null")

    override fun initKeys(query: JPAQuery<T>, page: Int) {
        if (page == 0) {
            initFirstId(query)
            initLastId(query)
            if (logger.isDebugEnabled()) {
                logger.debug { "First Key= $_currentId, Last Key= $_lastId" }
            }
        }
    }

    override fun initFirstId(query: JPAQuery<T>) {
        val clone = query.clone()
        val isGroupByQuery = isGroupByQuery(clone)

        _currentId = if (isGroupByQuery) {
            clone.select(field)
                .orderBy(if (expression.isAsc) field.asc() else field.desc())
                .fetchFirst()
        } else {
            clone.select(if (expression.isAsc) field.min() else field.max())
                .fetchFirst()
        }
    }

    override fun initLastId(query: JPAQuery<T>) {
        val clone = query.clone()
        val isGroupByQuery = isGroupByQuery(clone)

        _lastId = if (isGroupByQuery) {
            clone.select(field)
                .orderBy(if (expression.isAsc) field.desc() else field.asc())
                .fetchFirst()
        } else {
            clone.select(if (expression.isAsc) field.max() else field.min())
                .fetchFirst()
        }
    }

    override fun createQuery(query: JPAQuery<T>, page: Int): JPAQuery<T> {
        if (_currentId == null) {
            return query
        }

        return query
            .where(whereExpression(page))
            .orderBy(orderExpression())
    }

    private fun whereExpression(page: Int): BooleanExpression {
        return expression.where(field, page, _currentId!!)
            .and(if (expression.isAsc) field.loe(lastId) else field.goe(lastId))
    }

    private fun orderExpression(): OrderSpecifier<N> {
        return expression.order(field)
    }

    override fun resetCurrentId(item: T) {
        @Suppress("UNCHECKED_CAST")
        _currentId = getFieldValue(item) as? N ?: throw IllegalStateException("not found field value")

        if (logger.isDebugEnabled()) {
            logger.debug { "Current Select Key= $_currentId" }
        }
    }
}


class QuerydslNoOffsetStringOptions<T>(
    override val field: StringPath,
    expression: Expression
) : QuerydslNoOffsetOptions<T>(field, expression) {

    private var _currentId: String? = null
    private var _lastId: String? = null

    val currentId: String
        get() = _currentId ?: throw IllegalStateException("CurrentId is null")
    val lastId: String
        get() = _lastId ?: throw IllegalStateException("LastId is null")

    override fun initKeys(query: JPAQuery<T>, page: Int) {
        if (page == 0) {
            initFirstId(query)
            initLastId(query)
            if (logger.isDebugEnabled()) {
                logger.debug { "First Key= $_currentId, Last Key= $_lastId" }
            }
        }
    }

    override fun initFirstId(query: JPAQuery<T>) {
        val clone = query.clone()
        val isGroupByQuery = isGroupByQuery(clone)

        _currentId = if (isGroupByQuery) {
            clone.select(field)
                .orderBy(if (expression.isAsc) field.asc() else field.desc())
                .fetchFirst()
        } else {
            clone.select(if (expression.isAsc) field.min() else field.max())
                .fetchFirst()
        }
    }

    override fun initLastId(query: JPAQuery<T>) {
        val clone = query.clone()
        val isGroupByQuery = isGroupByQuery(clone)

        _lastId = if (isGroupByQuery) {
            clone.select(field)
                .orderBy(if (expression.isAsc) field.desc() else field.asc())
                .fetchFirst()
        } else {
            clone.select(if (expression.isAsc) field.max() else field.min())
                .fetchFirst()
        }
    }

    override fun createQuery(query: JPAQuery<T>, page: Int): JPAQuery<T> {
        if (_currentId == null) {
            return query
        }

        return query
            .where(whereExpression(page))
            .orderBy(orderExpression())
    }

    private fun whereExpression(page: Int): BooleanExpression {
        return expression.where(field, page, currentId)
            .and(if (expression.isAsc) field.loe(lastId) else field.goe(lastId))
    }

    private fun orderExpression(): OrderSpecifier<String> {
        return expression.order(field)
    }

    override fun resetCurrentId(item: T) {
        @Suppress("UNCHECKED_CAST")
        _currentId = getFieldValue(item) as? String ?: throw IllegalStateException("not found field value")

        if (logger.isDebugEnabled()) {
            logger.debug { "Current Select Key= $_currentId" }
        }
    }
}
