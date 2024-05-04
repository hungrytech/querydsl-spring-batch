package hungrytech.kotlinquerydslbatch.domain

import com.querydsl.jpa.impl.JPAQueryFactory
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Entity
class Contract(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0L,
    val name: String
) {


}


interface ContractRepository : JpaRepository<Contract, Long> {

    fun findByName(name: String): Contract?
}


@Repository
class ContractJpaRepository(
    private val contractRepository: ContractRepository,
    private val jpaQueryFactory: JPAQueryFactory
) : ContractRepository by contractRepository {

    fun querydls() {
        jpaQueryFactory.selectFrom(QContract.contract)

    }
}


class MyDsl(
    var fuck: String,
    var age: Int
) {
    fun good(): String = fuck
}

fun createDsl(receiver: MyDsl, dsl: MyDsl.() -> String): String {
    return receiver.dsl()
}
