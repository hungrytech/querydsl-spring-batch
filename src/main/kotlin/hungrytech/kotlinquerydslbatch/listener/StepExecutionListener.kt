package hungrytech.kotlinquerydslbatch.listener

import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.stereotype.Component

@Component
class StepExecutionListener: StepExecutionListener {

    override fun beforeStep(stepExecution: StepExecution) {
        stepExecution.jobParameters

    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
        return super.afterStep(stepExecution)
    }
}
