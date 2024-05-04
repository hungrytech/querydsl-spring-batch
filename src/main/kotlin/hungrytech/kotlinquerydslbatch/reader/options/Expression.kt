package hungrytech.kotlinquerydslbatch.reader.options

import com.querydsl.core.types.OrderSpecifier
import com.querydsl.core.types.dsl.BooleanExpression
import com.querydsl.core.types.dsl.NumberPath
import com.querydsl.core.types.dsl.StringPath

enum class Expression(
    private val where: WhereExpression,
    private val order: OrderExpression
) {
    ASC(WhereExpression.GT, OrderExpression.ASC),
    DESC(WhereExpression.LT, OrderExpression.DESC);

    val isAsc: Boolean
        get() = this == ASC

    fun where(id: StringPath, page: Int, currentId: String): BooleanExpression {
        return where.stringExpression(id, page, currentId)
    }

    fun <N> where(
        id: NumberPath<N>,
        page: Int,
        currentId: N
    ): BooleanExpression where N : Number, N : Comparable<*> = this.where.numberExpression(id, page, currentId)

    fun order(id: StringPath): OrderSpecifier<String> {
        return if (isAsc) id.asc() else id.desc()
    }

    fun <N> order(id: NumberPath<N>): OrderSpecifier<N> where N : Number, N : Comparable<*> {
        return if (isAsc) id.asc() else id.desc()
    }
}


enum class OrderExpression {
    ASC, DESC
}

enum class WhereExpression {
    GT,
    LT;


    fun <N> numberExpression(
        id: NumberPath<N>,
        page: Int,
        currentId: N
    ): BooleanExpression where N : Number, N : Comparable<*> = numberFunction(id, page, currentId)

    fun stringExpression(id: StringPath, page: Int, currentId: String): BooleanExpression = stringFunction(id, page, currentId)

    private fun <N> numberFunction(
        id: NumberPath<N>,
        page: Int,
        currentId: N
    ): BooleanExpression where N : Number, N : Comparable<*> =
        when (this) {
            GT -> if (page == 0) id.goe(currentId) else id.gt(currentId)
            LT -> if (page == 0) id.loe(currentId) else id.lt(currentId)
        }

    private fun stringFunction(id: StringPath, page: Int, currentId: String): BooleanExpression =
        when(this) {
            GT -> if (page == 0) id.goe(currentId) else id.gt(currentId)
            LT -> if (page == 0) id.loe(currentId) else id.lt(currentId)
        }
}
