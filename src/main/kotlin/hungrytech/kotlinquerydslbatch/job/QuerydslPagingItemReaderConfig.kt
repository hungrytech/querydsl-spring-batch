package hungrytech.kotlinquerydslbatch.job

import hungrytech.kotlinquerydslbatch.domain.Contract
import hungrytech.kotlinquerydslbatch.domain.QContract.contract
import hungrytech.kotlinquerydslbatch.domain.Settlement
import hungrytech.kotlinquerydslbatch.domain.SettlementRepository
import hungrytech.kotlinquerydslbatch.reader.QuerydslNoOffsetIdPagingItemReader
import hungrytech.kotlinquerydslbatch.reader.options.Expression
import hungrytech.kotlinquerydslbatch.reader.options.QuerydslNoOffsetNumberOptions
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemWriter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class QuerydslPagingItemReaderConfig(
    private val entityManagerFactory: EntityManagerFactory
) {

//    @Bean
//    fun querydslPagingItemReaderJob(jobRepository: JobRepository, platformTransactionManager: PlatformTransactionManager): Step {
//        return StepBuilder("querydslPagingItemReaderStep", jobRepository)
//            .chunk<Contract, Contract>(10, platformTransactionManager)
//            .reader(
////                QuerydslPagingItemReader(
////                    entityManagerFactory = entityManagerFactory,
////                    pageSize = 10
////                ) {
////                    jpaQueryFactory -> jpaQueryFactory.selectFrom(contract)
////                }
//
//            )
//            .processor(ItemProcessor { it })
//            .writer(
//                ItemWriter { println(it) }
//            )
//            .build()
//    }

    @Bean
    fun testJob(
        jobRepository: JobRepository,
        step: Step
    ): Job {
        return JobBuilder("testJob", jobRepository)
            .start(step)
            .build()
    }

    //    @StepScope
    @JobScope
    @Bean
    fun reIssuanceRefreshTokenBatchStep(
        jobRepository: JobRepository,
        platformTransactionManager: PlatformTransactionManager,
        reader: QuerydslNoOffsetIdPagingItemReader<Contract, Long>,
        writer: ItemWriter<Settlement>
    ): Step {
        return StepBuilder("reIssuanceRefreshToken", jobRepository)
            .chunk<Contract, Settlement>(
                30,
                platformTransactionManager
            )
            .reader(reader).processor {
                Settlement(name = it.name)
            }
            .writer(writer)
            .build()
    }

    @Bean
    fun itemReader(entityManagerFactory: EntityManagerFactory): QuerydslNoOffsetIdPagingItemReader<Contract, Long> {
        val option = QuerydslNoOffsetNumberOptions<Contract, Long>(
            field = contract.id,
            expression = Expression.DESC
        )

        return QuerydslNoOffsetIdPagingItemReader(
            pageSize = 30,
            entityManagerFactory = entityManagerFactory,
            options = option
        ) {
            jpaQueryFactory -> jpaQueryFactory.selectFrom(contract).from(contract)
        }
    }

    @Bean
    fun writer(settlementRepository: SettlementRepository): ItemWriter<Settlement> {
        return ItemWriter { chunk ->
            run {
                chunk.items.forEach {
                    println("item id: ${it.id}")
                }

                settlementRepository.saveAll(chunk.items)
            }
        }
    }
}
